package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)

//程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.读取Kafka ods_base_log 主题数据
        String topic = "ods_base_log";
        String groupId = "BaseLogApp210225";
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将数据转换为JSONObject
        OutputTag<String> dirtyOutPutTag = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyOutPutTag, value);
                }
            }
        });

        //获取脏数据并打印
        jsonObjDS.getSideOutput(dirtyOutPutTag).print("Dirty>>>>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.新老用户校验(状态编程)
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> flagState;

            @Override
            public void open(Configuration parameters) throws Exception {
                flagState = getRuntimeContext().getState(new ValueStateDescriptor<String>("flag-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //1.获取"is_new"标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //2.判断标记是否为"1"
                if ("1".equals(isNew)) {

                    String state = flagState.value();
                    if (state != null) {
                        //将数据中的"1"改为"0"
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //将数据写入状态
                        flagState.update("0");
                    }
                }

                //返回数据
                return value;
            }
        });

        //TODO 6.使用侧输出流对数据进行分流处理  页面-主流  启动-侧输出流  曝光-侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取启动相关数据
                String start = value.getString("start");

                //判断是否为启动数据
                if (start != null && start.length() > 0) {
                    //将数据写入start侧输出流
                    ctx.output(startTag, value);
                } else {

                    //将数据写入page主流
                    out.collect(value);

                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    //判断是否存在曝光数据
                    if (displays != null && displays.size() > 0) {

                        //获取页面id
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历曝光数据并写出到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);

                            //写出
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        //TODO 7.获取所有流的数据并将数据写入Kafka对应的主题
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");

        String startTopic = "dwd_start_log";
        String pageTopic = "dwd_page_log";
        String displayTopic = "dwd_display_log";

        startDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(startTopic));
        pageDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(pageTopic));
        displayDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(displayTopic));

        //TODO 8.启动任务
        env.execute();

    }

}

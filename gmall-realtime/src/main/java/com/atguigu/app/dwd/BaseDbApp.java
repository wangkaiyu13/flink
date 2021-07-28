package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyStringDeserializationSchema;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BaseDbApp {

    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.读取Kafka ods_base_db 主题数据
        String topic = "ods_base_db";
        String groupId = "BaseDbApp0225";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JSONObject         主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject)
                .filter(jsonObj -> {
                    String type = jsonObj.getString("type");
                    return !"delete".equals(type);
                });

        //TODO 4.通过FlinkCDC读取配置信息表,并封装为 广播流
        DebeziumSourceFunction<String> tableProcessStrSourceFunc = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210225-realtime")
                .tableList("gmall-210225-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(tableProcessStrSourceFunc);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonObjDS.connect(broadcastStream);
        broadcastConnectedStream.process(new BroadcastProcessFunction<JSONObject, String, Object>() {
            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Object> out) throws Exception {

            }
        });

        //TODO 6.处理连接流数据

        //TODO 7.获取Kafka数据流以及HBASE数据流写入对应的存储框架中

        //TODO 8.启动任务

    }

}

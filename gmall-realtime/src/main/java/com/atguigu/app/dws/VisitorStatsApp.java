package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VisitorStatsApp {

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

        //TODO 2.读取Kafka 3个主题的数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app0225";
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //TODO 3.统一数据格式

        //3.1 处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithUvDS = uvDS.map(line -> {
            //1.将数据转换为JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);
            //2.提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //3.2 处理UJ数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithUjDS = ujDS.map(line -> {
            //1.将数据转换为JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);
            //2.提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //3.3 处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithPvDS = pageViewDS.map(line -> {
            //1.将数据转换为JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);
            //2.提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //TODO 4.Union
        DataStream<VisitorStats> unionDS = visitStatsWithUvDS.union(visitStatsWithUjDS, visitStatsWithPvDS);

        //TODO 5.提取时间戳生成WaterMark

        //TODO 6.分组,开窗,聚合

        //TODO 7.将数据写入ClickHouse

        //TODO 8.启动任务
        env.execute();

    }

}

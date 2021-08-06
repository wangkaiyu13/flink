package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


//数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse

//程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> KeywordStatsApp -> ClickHouse
public class KeywordStatsApp {

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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取Kafka dwd_page_log 主题数据(提取时间戳生WaterMark)
        String groupId = "keyword_stats_app0225";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("CREATE TABLE page_log( " +
                "    `common` MAP<STRING,STRING>, " +
                "    `page` MAP<STRING,STRING>, " +
                "    `ts` BIGINT, " +
                "    `rt` As TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId));

        //TODO 3.过滤出搜索数据,并提取搜索关键词
        Table fullWordTable = tableEnv.sqlQuery("select " +
                "    page['item'] full_word, " +
                "    rt " +
                "from page_log " +
                "where page['item_type']='keyword' and page['item'] is not null");

        //TODO 4.注册UDTF并分词
        tableEnv.createTemporarySystemFunction("split_keyword", SplitFunction.class);
        Table splitWordTable = tableEnv.sqlQuery("SELECT word, rt FROM " + fullWordTable + ", LATERAL TABLE(split_keyword(full_word))");

        //TODO 5.词频统计(分组开窗聚合)
        Table resultTable = tableEnv.sqlQuery("select " +
                "    '" + GmallConstant.KEYWORD_SEARCH + "' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP() AS ts " +
                "from " + splitWordTable + " " +
                "group by word, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7.将数据写入ClickHouse
        keywordStatsDataStream.print(">>>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats_210225(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute();

    }
}
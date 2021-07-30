package com.atguigu;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TestDataStreamJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Table1> tA = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Table1(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Table1>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Table1>() {
                            @Override
                            public long extractTimestamp(Table1 element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));
        SingleOutputStreamOperator<Table2> tB = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Table2(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Table2>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Table2>() {
                            @Override
                            public long extractTimestamp(Table2 element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        SingleOutputStreamOperator<Tuple2<Table1, Table2>> result = tA.keyBy(Table1::getId)
                .intervalJoin(tB.keyBy(Table2::getId))
                .between(Time.seconds(-5), Time.seconds(5))  //最大的延迟时间
                .process(new ProcessJoinFunction<Table1, Table2, Tuple2<Table1, Table2>>() {
                    @Override
                    public void processElement(Table1 left, Table2 right, Context ctx, Collector<Tuple2<Table1, Table2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        result.print();

        env.execute();

    }
}
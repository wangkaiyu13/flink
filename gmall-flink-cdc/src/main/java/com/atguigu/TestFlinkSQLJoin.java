package com.atguigu;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class TestFlinkSQLJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<TableA> tA = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new TableA(fields[0], fields[1]);
                });
        SingleOutputStreamOperator<TableB> tB = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new TableB(fields[0], fields[1]);
                });

        tableEnv.createTemporaryView("t1", tA);
        tableEnv.createTemporaryView("t2", tB);

        //inner join  左表:disable            右表:OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,name,sex from t1 join t2 on t1.id = t2.id").print();

        //left join   左表:OnReadAndWrite     右表:OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,name,sex from t1 left join t2 on t1.id = t2.id").print();

        //right join  左表:OnCreateAndWrite   右表:OnReadAndWrite
        //tableEnv.executeSql("select t1.id,name,sex from t1 right join t2 on t1.id = t2.id").print();

        //full join  左表:OnReadAndWrite      右表:OnReadAndWrite
        tableEnv.executeSql("select t1.id,name,sex from t1 full join t2 on t1.id = t2.id").print();

    }

}

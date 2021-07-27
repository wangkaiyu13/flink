package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCSQL {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.通过DDL方式创建Source
        tableEnv.executeSql("CREATE TABLE base_trademark ( " +
                " id INT, " +
                " tm_name STRING, " +
                " logo_url STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'hadoop102', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '000000', " +
                " 'database-name' = 'gmall-210225-flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //TODO 3.查询数据并打印
        tableEnv.executeSql("select * from base_trademark").print();

        //TODO 4.启动任务
        env.execute();

    }

}

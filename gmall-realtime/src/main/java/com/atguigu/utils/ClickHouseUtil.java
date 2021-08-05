package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //获取所有的属性名称
                        Field[] declaredFields = t.getClass().getDeclaredFields();

                        //遍历字段信息,获取数据内容并给preparedStatement赋值
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            //获取字段名
                            Field field = declaredFields[i];

                            //设置私有属性值可访问
                            field.setAccessible(true);

                            //获取该字段上的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            //判断该注解是否存在
                            if (annotation == null) {
                                //获取值
                                Object value = null;
                                try {
                                    value = field.get(t);
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }

                                //给preparedStatement中"?"赋值
                                preparedStatement.setObject(i + 1 - offset, value);
                            } else {
                                offset++;
                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}

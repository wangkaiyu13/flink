package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    //声明线程池和Phoenix连接
    private ThreadPoolExecutor threadPoolExecutor;
    private Connection connection;

    //定义属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池和Phoenix连接
        threadPoolExecutor = ThreadPoolUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {

                //提取查询维度的id
                String id = getKey(input);

                //查询维度
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                //补充维度信息
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                //将关联好维度的数据输出到流中
                resultFuture.complete(Collections.singletonList(input));

            }
        });

    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}

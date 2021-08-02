package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {
    String getKey(T input) ;

    void join(T input, JSONObject dimInfo) throws ParseException;
}

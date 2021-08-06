package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.ProductStats;
import org.openjdk.jol.vm.VM;

import java.lang.reflect.Field;

public class Test {

    public static void main(String[] args) throws ClassNotFoundException {

//        ProductStats productStats = ProductStats.builder()
//                .build();
//        System.out.println(productStats.getRefund_amount());

//        Class<?> aClass = Class.forName("com.atguigu.bean.OrderWide");
//        Field[] declaredFields = aClass.getDeclaredFields();
//        for (Field declaredField : declaredFields) {
//            System.out.println(declaredField);
//        }

        JSONObject jsonObject = new JSONObject();
        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("a", "a");
        jsonObject1.put("b", "b");
        jsonObject1.put("v", "v");

        jsonObject.put("common", jsonObject1);

        JSONObject common = jsonObject.getJSONObject("common");
        JSONObject common1 = jsonObject.getJSONObject("common");

        System.out.println("打印数据地址--------");
        System.out.println(VM.current().addressOf(common));
        System.out.println(VM.current().addressOf(common1));
    }

}

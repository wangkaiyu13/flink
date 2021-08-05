package com.atguigu;

import com.atguigu.bean.ProductStats;

public class Test {

    public static void main(String[] args) {

        ProductStats productStats = ProductStats.builder()
                .build();
        System.out.println(productStats.getRefund_amount());


    }

}

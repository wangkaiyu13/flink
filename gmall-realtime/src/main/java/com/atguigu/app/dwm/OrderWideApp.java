package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class OrderWideApp {

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

        //TODO 2.读取Kafka dwd_order_info dwd_order_detail主题数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(line -> {

            OrderInfo orderInfo = JSONObject.parseObject(line, OrderInfo.class);

            //取出创建时间字段
            String create_time = orderInfo.getCreate_time();
            String[] dateTimeArr = create_time.split(" ");
            orderInfo.setCreate_date(dateTimeArr[0]);
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

            //时间戳字段
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {

            OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);

            //取出时间字段
            String create_time = orderDetail.getCreate_time();
            //时间戳字段
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))//设置的时间应该为最大的延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print("OrderWide>>>>>>>>>>");

        //TODO 5.关联维度信息
//        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //初始化连接
//            }
//
//            @Override
//            public OrderWide map(OrderWide orderWide) throws Exception {
//
//                Long user_id = orderWide.getUser_id();
//                JSONObject dimInfo = DimUtil.getDimInfo(co, "", user_id.toString());
//
//                return null;
//            }
//        });

        //TODO 6.将数据写入Kafka

        //TODO 7.启动任务
        env.execute();

    }

}

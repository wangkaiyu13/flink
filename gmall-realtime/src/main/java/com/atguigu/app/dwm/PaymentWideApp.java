package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

//数据流:web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim) -> FlinkApp -> Kafka(dwm)
//程  序:       mockDb -> Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix -> PaymentWideApp(OrderWideApp) -> Kafka
public class PaymentWideApp {

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

        //TODO 2.读取Kafka两个主题的数据
        String groupId = "payment_wide_group0225";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSONObject.parseObject(line, OrderWide.class)).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(orderWide.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentStrDS.map(line -> JSONObject.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        //TODO 4.双流JOIN并加工数据
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(2))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.将数据写入Kafka主题
        paymentWideDS.print(">>>>>>>>>");
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));

        //TODO 6.启动任务
        env.execute();

    }

}

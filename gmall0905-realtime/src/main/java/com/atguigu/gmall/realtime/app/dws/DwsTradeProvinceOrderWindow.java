package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception{
        //TODO 1.??????????????????
        //1.1 ?????????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 ???????????????
        env.setParallelism(4);
        //TODO 2.????????????????????????
        //2.1 ???????????????
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 ???????????????????????????
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 ??????job??????????????????????????????
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 ?????????????????????????????????????????????
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 ??????????????????
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 ??????????????????
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //2.7 ????????????hadoop?????????
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.???kafka????????????????????????
        //3.1 ???????????????????????????????????????
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_group";
        //3.2 ?????????????????????
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 ???????????? ????????????
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //{"id":"2741","order_id":"1137","user_id":"326","sku_id":"3","sku_name":"??????10","province_id":"24",
        // "activity_id":null,"activity_rule_id":null,"coupon_id":null,"date_id":"2023-02-18",
        // "create_time":"2023-02-18 10:49:53","source_id":null,"source_type":"2401",
        // "source_type_name":"????????????","sku_num":"2","split_original_amount":"11998.0000",
        // "split_activity_amount":null,"split_coupon_amount":null,"split_total_amount":"11998.0",
        // "ts":"1678070994"}
        //kafkaStrDS.print(">>>");

        //TODO 4.?????????null????????????????????????????????????????????????    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                if (StringUtils.isNotEmpty(jsonStr)) {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                }
            }
        });
        //TODO 5.???????????????(order_detail_id)????????????
        KeyedStream<JSONObject, String> orderDetaiIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        //TODO 6.??????     ?????? + ??????
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetaiIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastValueState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastValueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastValueState.value();
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastValueState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        //TODO 7.??????????????????????????????     jsonObj->???????????????
        // distinctDS.print(">>>");
        SingleOutputStreamOperator<TradeProvinceOrderBean> orderBeanDS = distinctDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        String provinceId = jsonObj.getString("province_id");
                        String splitTotalAmount = jsonObj.getString("split_total_amount");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String orderId = jsonObj.getString("order_id");

                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderIdSet(new HashSet(Collections.singleton(orderId)))
                                .orderAmount(new BigDecimal(splitTotalAmount))
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
        //TODO 8.??????Watermark??????????????????????????????
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderBean orderBean, long recordTimestamp) {
                                        return orderBean.getTs();
                                    }
                                }
                        )
        );
        //TODO 9.?????????????????????????????????
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS
                = withWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 10.??????
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS
                = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 11.????????????
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String groupId, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        for (TradeProvinceOrderBean orderBean : input) {
                            orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                            orderBean.setTs(System.currentTimeMillis());
                            out.collect(orderBean);
                        }
                    }
                }
        );
        reduceDS.print("????????????");
        //TODO 12.???????????????????????????
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province") {
                    @Override
                    public void join(TradeProvinceOrderBean orderBean, JSONObject dimInfoJsonObj) {
                        orderBean.setProvinceName(dimInfoJsonObj.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                600, TimeUnit.SECONDS
        );
        //TODO 13.??????????????????Clickhouse???
        withProvinceDS.print("?????????????????????");
        withProvinceDS.addSink(
                MyClickHouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}

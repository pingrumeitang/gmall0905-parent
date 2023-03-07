package com.atguigu.gmall.realtime.app.dws;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置(略)

        //TODO 3.从kafka主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_sku_order_group";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据 并封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 4.对读取的数据进行类型转换并过滤null消息   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );
        //{"create_time":"2023-02-18 11:38:39","sku_num":"3","activity_rule_id":"5",
        // "split_original_amount":"207.0000","split_coupon_amount":"70.0","sku_id":"31",
        // "source_type":"2401","date_id":"2023-02-18","source_type_name":"用户查询",
        // "coupon_id":"2","user_id":"97","province_id":"15","activity_id":"3",
        // "sku_name":"CAREMiLLE珂曼","id":"343","order_id":"144","split_activity_amount":"21.0",
        // "split_total_amount":"116.0","ts":"1677814719"}
        // jsonObjDS.print(">>>");

        //TODO 5.去重
        //5.1 按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        /*
        //5.2 去重 思路1：状态 + 定时器
        orderDetailIdKeyedDS.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> lastValueState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastValueState
                        = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("lastValueState",JSONObject.class));
                }

                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject lastJsonObj = lastValueState.value();
                    //状态中是否存在数据
                    if(lastJsonObj == null){
                        //当前这条数据是这个订单明细对应的第一条数据
                        lastValueState.update(jsonObj);
                        //注册一个定时器
                        long processingTime = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(processingTime +5000L);
                    }else{
                        //说明有重复数据产生，用当前数据的时间和状态中的时间进行比较
                        //注意：这里是伪代码，row_ts代表的是dwd聚合的时间，不是下单的事件时间
                        String rowTs1 = lastJsonObj.getString("row_ts");
                        String rowTs2 = jsonObj.getString("row_ts");
                        if(rowTs1.compareTo(rowTs2)<=0){
                            lastValueState.update(jsonObj);
                        }
                    }
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject jsonObj = lastValueState.value();
                    out.collect(jsonObj);
                    lastValueState.clear();
                }
            }
        );*/
        //5.2 去重 思路2：状态 + 抵消(如果有数据重复，将影响度量值的字段取反后传递，在下游进行抵消)
        SingleOutputStreamOperator<JSONObject> processDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        this.lastValueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastValueState.value();
                        if(lastJsonObj != null){
                            //说明以及出现重复了
                            String splitOriginalAmount = jsonObj.getString("split_original_amount");
                            String splitTotalAmount = jsonObj.getString("split_total_amount");
                            String splitActivityAmount = jsonObj.getString("split_activity_amount");
                            String splitCouponAmount = jsonObj.getString("split_coupon_amount");

                            lastJsonObj.put("split_original_amount","-" + splitOriginalAmount);
                            lastJsonObj.put("split_total_amount","-" + splitTotalAmount);
                            lastJsonObj.put("split_activity_amount", StringUtils.isEmpty(splitActivityAmount)
                                    ?"0.0":"-" + splitActivityAmount);
                            lastJsonObj.put("split_coupon_amount", StringUtils.isEmpty(splitCouponAmount)
                                    ?"0.0":"-" + splitCouponAmount);
                            out.collect(lastJsonObj);
                        }
                        lastValueState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        //TODO 6.再次对流中数据进行转换        jsonObj->实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> orderBeanDS = processDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {

                        //{"create_time":"2023-02-18 11:38:39","sku_num":"3","activity_rule_id":"5",
                        // "split_original_amount":"207.0000","split_coupon_amount":"70.0","sku_id":"31",
                        // "source_type":"2401","date_id":"2023-02-18","source_type_name":"用户查询",
                        // "coupon_id":"2","user_id":"97","province_id":"15","activity_id":"3",
                        // "sku_name":"CAREMiLLE珂曼","id":"343","order_id":"144","split_activity_amount":"21.0",
                        // "split_total_amount":"116.0","ts":"1677814719"}

                        String skuId = jsonObj.getString("sku_id");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String splitOriginalAmount = jsonObj.getString("split_original_amount");
                        String splitTotalAmount = jsonObj.getString("split_total_amount");
                        String splitActivityAmount = jsonObj.getString("split_activity_amount");
                        String splitCouponAmount = jsonObj.getString("split_coupon_amount");
                        TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(new BigDecimal(splitOriginalAmount))
                                .orderAmount(new BigDecimal(splitTotalAmount))
                                .activityAmount(new BigDecimal(StringUtils.isEmpty(splitActivityAmount)
                                        ? "0.0" : splitActivityAmount))
                                .couponAmount(new BigDecimal(StringUtils.isEmpty(splitCouponAmount) ? "0.0" : splitCouponAmount))
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
        // orderBeanDS.print(">>>>");
        //TODO 7.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean orderBean, long recordTimestamp) {
                                        return orderBean.getTs();
                                    }
                                }
                        )
        );
        //TODO 8.按照统计的维度skuId进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS
                = withWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 9.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS
                = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 10.聚合计算
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                        value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String groupId, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        for (TradeSkuOrderBean orderBean : input) {
                            orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            orderBean.setTs(System.currentTimeMillis());
                            out.collect(orderBean);
                        }
                    }
                }
        );
        //TradeSkuOrderBean(stt=2023-03-04 09:06:30, edt=2023-03-04 09:06:40, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=11, skuName=null, spuId=null, spuName=null, originalAmount=57379.0000, activityAmount=2050.0, couponAmount=0.0, orderAmount=55329.0, ts=1677892043671)
        // reduceDS.print(">>");
        //TODO 11.关联sku维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
            new MapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                    //通过流中的对象获取要关联的维度的主键
                    String skuId = orderBean.getSkuId();
                    //根据维度的主键到维度表中获取对应的维度对象
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfo("dim_sku_info", skuId);
                    if(dimInfoJsonObj != null){
                        //将维度属性补充到流中的对象上
                        // ID,SPU_ID,PRICE,SKU_NAME,SKU_DESC,WEIGHT,TM_ID,CATEGORY3_ID,SKU_DEFAULT_IMG,IS_SALE,CREATE_TIME
                        orderBean.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                        orderBean.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
                        orderBean.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                        orderBean.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                    }
                    return orderBean;
                }
            }
        );
        withSkuInfoDS.print(">>>>");*/
        //将异步I/O操作应用于DataStream作为DataStream的一次转换操作
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info") {
                    @Override
                    public void join(TradeSkuOrderBean orderBean, JSONObject dimInfoJsonObj) {
                        orderBean.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                        orderBean.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
                        orderBean.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                        orderBean.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSkuInfoDS.print("关联sku数据");

        //TODO 12.关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSPUDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_spu_info") {
                    @Override
                    public void join(TradeSkuOrderBean obj, JSONObject dimInfoJsonObj) {
                        obj.setSpuName(dimInfoJsonObj.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean obj) {
                        return obj.getSkuId();
                    }
                }, 60, TimeUnit.SECONDS
        );
        //TODO 13.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSPUDS,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_trademark") {
                    @Override
                    public void join(TradeSkuOrderBean obj, JSONObject dimInfoJsonObj) {
                                obj.setTrademarkName(dimInfoJsonObj.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean obj) {
                        return obj.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //TODO 14.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean,JSONObject jsonObj)  {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
        //TODO 15.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean,JSONObject jsonObj)  {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
        //TODO 16.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean,JSONObject jsonObj)  {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
        //TODO 17.将关联聚合的结果写到Clickhouse中
        withCategory1Stream.print(">>>>");
        withCategory1Stream.addSink(
                MyClickHouseUtil.getSinkFunction("insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TrafficPageViewBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow  {
    public static void main(String[] args) throws Exception{
        //TODO 1.基本环境的准备
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //TODO 2.检查点的相关设置
        //TODO 3. 从kafka的页面日志中读取数据
        //声明消费者主题以及消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_ch_ar_isnew_group";
        //创建kafka消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //将数据分装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //TODO 4. 对流中的数据进行类型的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                return jsonObj;
            }
        });
        jsonObjDS.print("jsonobj数据流");

        //TODO 5. 按照mid(设备id) 进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
        keyedDS.print("分组之后的数据");
        //TODO 6. 将度量值进行计算,并将其封装为一个实体类对象   需要进行状态编程,进行对独立访客的计算
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            //声明值键控状态,用来保存该设备的上次访问日期
            private ValueState<String> lastViewDate;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastViewDate", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastViewDate = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<TrafficPageViewBean> out) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                //获取维度
                String vc = common.getString("vc");//版本号
                String ch = common.getString("ch");//渠道
                String ar = common.getString("ar");//地区
                String is_new = common.getString("is_new");//新老访客
                //判断是否为独立访客
                Long uvCn = 0L;
                //从状态获取该设备的访问日期,
                String lastView = lastViewDate.value();
                //获取当前访问日期
                Long ts = jsonObject.getLong("ts");
                String currentViewDate = DateFormatUtil.toDate(ts);
                if (StringUtils.isEmpty(lastView) || !lastView.equals(currentViewDate)) {
                    uvCn = 1L;
                    lastViewDate.update(currentViewDate);
                }
                //判断是否为新的会话
                String last_page_id = page.getString("last_page_id");
                Long svCn = StringUtils.isEmpty(last_page_id) ? 1L : 0L;
                //浏览次数的统计
                Long pvCn = 1L;
                //持续访问时间的计算
                Long duringtime = page.getLong("during_time");
                TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean("", "", vc, ch, ar, is_new, uvCn, svCn, pvCn
                        , duringtime, ts);
                out.collect(trafficPageViewBean);
            }
        });
        beanDS.print("封装为实体类的数据流");
        //TODO 7. 指定watermark 并提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }));
        withWatermarkDS.print("指定水位线");

        //TODO 8. 分组  按照其维度进行分组 防止后期进行维度聚合时维度的丢失,
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDimDS = withWatermarkDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean in) throws Exception {
                return new Tuple4<>(in.getVc(), in.getCh(), in.getAr(), in.getIsNew());
            }
        });
        keyedDimDS.print("按照维度进行分组");
        //TODO 9. 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDimDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 10.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                return t1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                for (TrafficPageViewBean viewBean : iterable) {
                    viewBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                    viewBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                    viewBean.setTs(System.currentTimeMillis());
                    collector.collect(viewBean);
                }
            }
        });
        //TODO 11.将结果写到CK中
        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?)"));
            env.execute();

    }
}

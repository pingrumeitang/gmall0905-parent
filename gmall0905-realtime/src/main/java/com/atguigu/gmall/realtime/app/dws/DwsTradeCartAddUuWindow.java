package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.CartAddUuBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 交易域加购独立用户数
 * Flink API
 */

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) {
        //TODO 1.基本环境的准备;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点的设置
        //TODO 3.消费kafka dwd 加购明细主题数据
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_group";
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //将消费到的数据封装为数据流
        DataStreamSource<String> kafkaSourceDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //kafkaSourceDS.print("从加购主题中读取的数据");
        //TODO 4.转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                return jsonObj;
            }
        });
        //TODO 5.设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts") * 1000;
                    }
                }));
        //TODO 6.按照user_id进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("user_id");
            }
        });
        //TODO 7.过滤独立用户加购记录(使用状态编程)
        SingleOutputStreamOperator<JSONObject> processDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            //定义状态来存储该用户上次加购日期
            private ValueState<String> lastCartAddDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartAddDateState", String.class);
                lastCartAddDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                String lastCartAddDate = lastCartAddDateState.value();
                Long ts = jsonObject.getLong("ts") * 1000;
                String currentDate = DateFormatUtil.toDate(ts);
                if (StringUtils.isEmpty(lastCartAddDate) || !lastCartAddDate.equals(currentDate)) {
                    collector.collect(jsonObject);
                    lastCartAddDateState.update(currentDate);
                }
            }
        });
        //processDS.print("过滤掉重复数据");//这里过滤掉的是同一个人同一天加购的数据
        //TODO 8.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //processDS.print("开窗之后的数据:");
        //TODO 9.聚合
        SingleOutputStreamOperator<CartAddUuBean> aggDS = windowDS.aggregate(new AggregateFunction<JSONObject, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(JSONObject jsonObject, Long aLong) {
                return ++aLong;
            }

            @Override
            public Long getResult(Long aLong) {
                return aLong;
            }

            @Override
            public Long merge(Long aLong, Long acc1) {
                return null;
            }
        }, new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<CartAddUuBean> collector) throws Exception {
                for (Long aLong : iterable) {
                    collector.collect(new CartAddUuBean(DateFormatUtil.toYmdHms(timeWindow.getStart()),
                            DateFormatUtil.toYmdHms(timeWindow.getEnd()),
                            aLong,
                            System.currentTimeMillis()));
                }
            }
        });
        //TODO 10.将数据写入到CK中
        aggDS.print("成功数据");
        aggDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

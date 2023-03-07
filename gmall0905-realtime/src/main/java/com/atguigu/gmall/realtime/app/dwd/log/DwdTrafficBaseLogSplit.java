package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.avro.ValidateLatest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficBaseLogSplit {
    public static void main(String[] args)throws Exception {
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.设置检查点
        //代表多长时间发出一个检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //设置job取消之后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //设置Hadoop的操作用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.从kafka主题中消费数据
        //声明消费主题以及消费者组
        String topic = "topic_log";
        String groupId = "dwd_traffic_log_split_group";
        //创建消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //消费数据封装为数据流
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //TODO 4.对流中的数据进行简单的ETL以及数据类型的转换   将脏数据写入到测输出流中
        //定义测输出流标签  (加后面的花括号,防止类型擦除)
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果是脏数据将会捕捉异常,输出到测输出流中
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });
        jsonObjDS.print("正常数据");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据");
        //TODO 5.将测输出流中的脏数据写入到Kafka主题中
        dirtyDS.sinkTo(MyKafkaUtil.getKafkaSink("dirty_data","dirty"));
        //TODO 6.对新老访客进行数据的修复   状态编程
        //按照mid进行分组,一亿条数据大约800M的内存
        KeyedStream<JSONObject, String> keyedDs = jsonObjDS.keyBy(
                jsonObject -> jsonObject.getJSONObject("common").getString("mid")
        );
        // 进行数据的修复,对is_new进行修复,及新老访客
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDs.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //定义一个值键控状态用于存储每一访客的上一次访问日期
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                         lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                                "lastVisitDateState", String.class ));
                       /* ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);*/

                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取当前数据流的有用户访客状态
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();//获取状态变量里面的时间
                        Long ts = jsonObject.getLong("ts");
                        //准备一个日期格式化工具类
                        String curVisitDate = DateFormatUtil.toDate(ts);
                        if ("1".equals(isNew)) {//当前状态为新用户
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //证明其为新用户
                                lastVisitDateState.update(curVisitDate);
                            } else {//如果不为空,
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {//此时数据中用户状态为0,为老用户
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                Long yesterDayMS = ts - 24 * 60 * 60 * 1000;
                                String yesterDay = DateFormatUtil.toDate(yesterDayMS);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        return jsonObject;
                    }
                }
        );
        fixedDS.print("修复过的数据");
        //TODO 7.分流   错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流 页面日志-主流
        //定义值类型标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流 
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        //处理错误日志
                        JSONObject errJsonObj = jsonObject.getJSONObject("err");
                        if (errJsonObj != null) {
                            context.output(errTag, errJsonObj.toJSONString());//用这个方法可以少一次方法调用,即压站
                            jsonObject.remove("err");
                        }
                        //处理启动日志
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        if (startJsonObj != null) {
                            context.output(startTag, startJsonObj.toJSONString());
                        } else {
                            //处理页面日志
                            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");
                            //处理曝光数据
                            JSONArray displayArr = jsonObject.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJSONObject = displayArr.getJSONObject(i);
                                    JSONObject newdisplayJsonObj = new JSONObject();
                                    newdisplayJsonObj.put("common", commonJsonObj);
                                    newdisplayJsonObj.put("page", pageJsonObj);
                                    newdisplayJsonObj.put("ts", ts);
                                    newdisplayJsonObj.put("display", displayJSONObject);
                                    context.output(displayTag, newdisplayJsonObj.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }
                            //处理动作日志
                            JSONArray actionJsonObjArr = jsonObject.getJSONArray("actions");
                            if (actionJsonObjArr != null && actionJsonObjArr.size() > 0) {
                                for (int i = 0; i < actionJsonObjArr.size(); i++) {
                                    JSONObject actionJsonObj = actionJsonObjArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            //页面日志输入到主流中
                            collector.collect(jsonObject.toJSONString());
                        }

                    }
                }
        );
        //TODO 8.将不同流的数据写到kafka的主题中
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_page_log","page"));
        errDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_err_log","err"));
        startDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_start_log","start"));
        displayDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_display_log","display"));
        actionDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_action_log","action"));

        env.execute();
    }
}

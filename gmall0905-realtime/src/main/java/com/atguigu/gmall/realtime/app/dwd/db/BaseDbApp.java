package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.BaseDbTableProcessFunction;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class BaseDbApp {
    public static void main(String[] args) {
        //TODO 1.准备基本的环境
        //指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
/*        //TODO 2.检查点的设置
        //开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        //设置job取消时检查点是否保留,
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000L);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.days(30),Time.seconds(3)));
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8082/gmall/ck");
        //设置hadoop操作用户
        System.setProperty("HADOOP_USER_NAME","atguigu");*/
        //TODO 3.从KAFKA中读取数据,进行数据的转换以及简单的ETL
        String topic = "topic_db";
        String groupId = "base_db_group";
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> JsonDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = JsonDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    if (!jsonObj.getString("type").equals("bootstrap-start")
                            && !jsonObj.getString("type").equals("bootstrap-complete")
                            && !jsonObj.getString("type").equals("bootstrap-insert")) {
                        out.collect(jsonObj);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        jsonObjDS.print("主流数据");
        //TODO 4.使用FlinkCDC从mysql中读取配置信息
        Properties props = new Properties();
        props.setProperty("useSSL","false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .jdbcProperties(props)
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> mySQLDS
                = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mySQLDS.print("广播流数据");
        //TODO 5.将配置信息进行广播
        MapStateDescriptor<String, TableProcess> mapstate = new MapStateDescriptor<>("mapstate", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mySQLDS.broadcast(mapstate);
        //TODO 6.将广播流数据和主流数据进行关联
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 7.对关联之后的数据进行处理
        SingleOutputStreamOperator<JSONObject> DS = connectDS.process(new BaseDbTableProcessFunction(mapstate));
            DS.print("最终数据");
        //TODO 8.将流中的数据写到kafka不同的主题中
         DS.sinkTo(MyKafkaUtil.<JSONObject>getKafkaSinkBySchema("base_db", new KafkaRecordSerializationSchema<JSONObject>() {
             @Override
             public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                 String topic = jsonObject.getString("sink_table");
                 jsonObject.remove("sink_table");
                 return new ProducerRecord<byte[], byte[]>(topic,jsonObject.toJSONString().getBytes());
             }
         }));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

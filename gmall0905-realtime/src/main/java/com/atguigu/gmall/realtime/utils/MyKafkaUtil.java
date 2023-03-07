package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;


public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    //获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_SERVER)//kafka的地址
            .setTopics(topic)
            .setGroupId(groupId)
            //从状态中维护的偏移量开始消费，如果还没有维护，从分区最新的偏移量位置开始消费
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
            .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            //注意：如果使用SimpleStringSchema进行反序列化，如果从主题中读到了空消息，没办法处理
            // .setValueOnlyDeserializer(new SimpleStringSchema())
            .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                @Override
                public String deserialize(byte[] message) throws IOException {
                    if (message != null) {
                        return new String(message);
                    }
                    return null;
                }

                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }

                @Override
                public TypeInformation<String> getProducedType() {
                    return TypeInformation.of(String.class);
                }
            })
            .build();
        return kafkaSource;
    }
    //获取kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic,String transactionalIdPrefix){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)  //设置kafka服务集群
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")//事务的更新时间要比检查点的更新时间长
                //在同一个flink的job中,如果多条数据流向Kafka中发送数据,每一条流都会开启事务,都有相同的规则,会发生冲突
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .build();
        return kafkaSink;
    }
    //获取从topic_db主题中读取数据并创建动态表的建表语句
    public static String getTopicDbDDL(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `ts` string,\n" +
                "  `data` MAP<string, string>,\n" +
                "  `old` MAP<string, string>,\n" +
                "  proc_time as proctime()\n" +
                ") " + getKafkaDDL("topic_db", groupId);
    }

    //获取kafka连接器的连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'properties.auto.offset.reset' = 'latest',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取upsert-kafak连接器连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
    //获取kafkaSink
    public static <T>KafkaSink<T> getKafkaSinkBySchema(String transactionalIdPrefix,KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema){
        KafkaSink<T> kafkaSink = KafkaSink
                .<T>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 +"")
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .build();
        return kafkaSink;
    }
}

package com.atguigu.gmall.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties props = new Properties();
        props.setProperty("useSSL","false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .databaseList("gmall_config") // set captured database
            .tableList("gmall_config.t_user") // set captured table
            .jdbcProperties(props)
            .username("root")
            .password("000000")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

        //"op":"c"： {"before":null,"after":{"id":4,"name":"cls","age":18},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676859087000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":368,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1676859087110,"transaction":null}
        //"op":"r":  {"before":null,"after":{"id":33,"name":"kwp","age":30},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1676858650214,"transaction":null}
        //"op":"u"： {"before":{"id":4,"name":"cls","age":18},"after":{"id":4,"name":"xzls","age":18},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676859132000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":662,"row":0,"thread":null,"query":null},"op":"u"/,"ts_ms":1676859132961,"transaction":null}
        //"op":"d"： {"before":{"id":4,"name":"xzls","age":18},"after":null,"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676859180000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":972,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1676859180023,"transaction":null}
        env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

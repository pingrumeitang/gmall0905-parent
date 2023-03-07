package com.atguigu.gmall.realtime.utils;
import com.atguigu.gmall.realtime.beans.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
public class MyClickHouseUtil {
    //获取SinkFunction
    public static <T>SinkFunction<T> getSinkFunction(String sql){

        SinkFunction<T> sinkFunction = JdbcSink.sink(
                // insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //给问号占位符赋值    将流中的对象相关的属性给问号占位符赋上
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
                        //遍历类中的所有的属性
                        int skipNum = 0;
                        for (int i = 0; i < fieldArr.length; i++) {
                            //获取属性对象
                            Field field = fieldArr[i];

                            //判断属性是否需要向CK中保存
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if(transientSink != null){
                                skipNum ++;
                                continue;
                            }

                            //设置私有属性的访问权限
                            field.setAccessible(true);
                            try {
                                //获取属性的值
                                Object fieldValue = field.get(obj);
                                //将属性的值赋给对应的问号占位符
                                ps.setObject(i + 1 - skipNum,fieldValue);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
        return sinkFunction;
    }
}

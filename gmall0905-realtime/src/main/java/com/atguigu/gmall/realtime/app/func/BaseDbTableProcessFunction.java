package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    //提前将配置信息加载到内存中
    private Map<String,TableProcess> configmap = new HashMap<>();
    //连接MySQL提前读取配置信息
    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //获取连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?user=root&password=000000&useUnicode=true&characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        //准备sql执行环境
        String sql = "select * from gmall_config.table_process where sink_type = 'dwd'";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        //执行sql
        ResultSet resultSet = preparedStatement.executeQuery();
        //处理结果集
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()){
            //创建json对象用来接受查询的结果集
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String tableName = metaData.getColumnName(i);//获取表名
                Object columnValue = resultSet.getObject(i);
                jsonObject.put(tableName,columnValue);
            }
            //将json对象转化为TableProcess对象
            TableProcess tableProcess = jsonObject.toJavaObject(TableProcess.class);
            String sourceTable = tableProcess.getSourceTable();
            String sourceType = tableProcess.getSourceType();
            String key = sourceTable + "_" + sourceType;
        }
        resultSet.close();
        preparedStatement.close();
        conn.close();
    }
    //处理主流数据
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
     //获取当前数据的表格名
        String tableName = jsonObject.getString("table");
        //获取其操作类型
        String type = jsonObject.getString("type");
        //将表名与操作类型作为key
        String key = tableName + "_" + type;
        //过去广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //根据key到广播状态和configmap中准找配置信息
        TableProcess tableProcess = null;
        if ((tableProcess= broadcastState.get(key))!= null || (tableProcess= configmap.get(key))!=null){
            JSONObject dataJson = jsonObject.getJSONObject("data");
            filterColumn(dataJson,tableProcess.getSinkColumns());
            Long ts = jsonObject.getLong("ts");
            dataJson.put("ts",ts);
            //在向下游传递数据前，补充输出的目的地
            dataJson.put("sink_table",tableProcess.getSinkTable());
            collector.collect(dataJson);
        }
    }
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
     //处理广播流数据
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取对配置表中数据的操作类型
        String op = jsonObj.getString("op");
        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //判断对配置表做了什么的操作
        if ("d".equals(op)) {
            //如果从配置表中删除了配置，将配置从广播状态以及维护的map集合中删除掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sinkType = before.getSinkType();
            if ("dwd".equals(sinkType)) {
                String sourceTable = before.getSourceTable();
                String sourceType = before.getSourceType();
                String key = sourceTable + "_" + sourceType;
                broadcastState.remove(key);
                configmap.remove(key);
            }
        } else {
            //如果对配置表做了删除外的其它操作，将配置从添加或者更新到广播状态以及维护的map集合中
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dwd".equals(sinkType)) {
                String sourceTable = after.getSourceTable();
                String sourceType = after.getSourceType();
                String key = sourceTable + "_" + sourceType;
                broadcastState.put(key, after);
                configmap.put(key,after);
            }
        }
    }
}

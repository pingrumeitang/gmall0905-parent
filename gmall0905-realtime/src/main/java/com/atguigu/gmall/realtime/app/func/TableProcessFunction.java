package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess>      mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    private Map<String,TableProcess> configMap = new HashMap<>();
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?" +
                "user=root&password=000000&useUnicode=true&characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        //创建数据库操作对象
        String sql = "select * from gmall_config.table_process where sink_type='dim'";
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行SQL语句
        ResultSet rs = ps.executeQuery();
        //处理结果集
        ResultSetMetaData metaData = rs.getMetaData();
        while(rs.next()){
            //创建一个jsonObj，用于封装查询的一条结果
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            //利用fastJson，将jsonObj转换为实体类对象
            TableProcess tableProcess = JSON.toJavaObject(jsonObj, TableProcess.class);
            //将封装的实体对象放到configMap集合中
            configMap.put(tableProcess.getSourceTable(),tableProcess);
        }
        //释放资源
        rs.close();
        ps.close();
        conn.close();
    }

    //处理主流的业务数据
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取主流中的表名
        String tableName = value.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //通过表名判断广播状态中是否有维度表的数据
        TableProcess tableProcess = null;


        //如果不为空证明为维度的数据
        if ((tableProcess = broadcastState.get(tableName))!=null || (tableProcess=configMap.get(tableName))!=null){
            //得到主流数据中的data 数据
            JSONObject data = value.getJSONObject("data");
            //过滤掉不需要的字段,通过建表时的字段进行判断,即广播状态中的vlaue值
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data,sinkColumns);
            //将表名加入到主流数据中,便于知道数据最后插入哪个表
            data.put("sink_table",tableProcess.getSinkTable());
            data.put("type",value.getString("type"));
            out.collect(data);
        }

    }
    //过滤不需要的字段
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));

    }

    //处理广播流中的配置数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        //"op":"r": {"before":null,"after":{"source_table":"activity_info","source_type":"ALL","sink_table":"dim_activity_info","sink_type":"dim","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_pk":"id","sink_extend":null},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1676877345748,"transaction":null}
        //"op":"c": {"before":null,"after":{"source_table":"a","source_type":"ALL","sink_table":"aa","sink_type":"dim","sink_columns":"id,name","sink_pk":"id","sink_extend":null},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676877468000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":727651,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1676877468250,"transaction":null}
        //"op":"u": {"before":{"source_table":"a","source_type":"ALL","sink_table":"aa","sink_type":"dim","sink_columns":"id,name","sink_pk":"id","sink_extend":null},"after":{"source_table":"a","source_type":"ALL","sink_table":"aa","sink_type":"dim","sink_columns":"id,name,age","sink_pk":"id","sink_extend":null},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676877507000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":727985,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1676877507224,"transaction":null}
        //"op":"d": {"before":{"source_table":"a","source_type":"ALL","sink_table":"aa","sink_type":"dim","sink_columns":"id,name,age","sink_pk":"id","sink_extend":null},"after":null,"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676877536000,"snapshot":"false","db":"gmall0905_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":728355,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1676877536960,"transaction":null}
        // System.out.println(jsonStr);
        //为了操作方便  将从广播流中读取的字符串进行类型转换    jsonObj
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取对配置表的操作类型
        String op = jsonObj.getString("op");
        if ("d".equals(op)) {
            //如果对配置表进行删除操作  将对应的配置信息从广播状态中删除掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sinkType = before.getSinkType();
            if ("dim".equals(sinkType)) {
                broadcastState.remove (before.getSourceTable());
                configMap.remove(before.getSourceTable());
            }
        } else {
            //如果对配置表做的是删除外的其它操作  将对应的配置信息从添加或者更新到广播状态中
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dim".equals(sinkType)) {
                //获取业务数据库表名
                String sourceTable = after.getSourceTable();
                //获取phoenix中建表表名
                String sinkTable = after.getSinkTable();
                //获取表中字段
                String sinkColumns = after.getSinkColumns();
                //获取建表主键
                String sinkPk = after.getSinkPk();
                //获取建表扩展
                String sinkExtend = after.getSinkExtend();

                //提前创建phoenix中的维度表
                checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                broadcastState.put(sourceTable, after);
                configMap.put(sourceTable, after);
            }
        }
    }

    //维度表的创建
    private void checkTable(String sinkTable, String sinkColumns, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.PHOENIX_SCHEMA +"." + sinkTable + "(");
        String[] columnArr = sinkColumns.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            if(column.equals(pk)){
                createSql.append(column + " varchar primary key");
            }else{
                createSql.append(column + " varchar");
            }
            if(i < columnArr.length - 1){
                createSql.append(",");
            }
        }
        createSql.append(") " + ext);
        System.out.println("Phoenix中执行的建表语句:" + createSql);

        Connection conn = null;
        PreparedStatement ps = null;
        try {
            //获取连接
            conn = DruidDSUtil.getConnection();
            //获取数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //释放资源
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}

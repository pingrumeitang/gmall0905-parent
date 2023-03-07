package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Set;

public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //拼接upsert语句，使用jdbc的方式，将流中的数据插入到phoenix表中
        //流中数据：{"tm_name":"三星6","sink_table":"dim_base_trademark","id":1}
        //要拼接upsert语句：upsert into 库名.表名 (a,b,c) values('aa','bb','cc');
        //根据流中的jsonObj获取插入的目的地表
        String sinkTable = jsonObj.getString("sink_table");
        //获取到输出目的地之后，可以将该属性从jsonObj中删除掉   {"tm_name":"三星6","id":1}
        jsonObj.remove("sink_table");
        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        Set<String> keys = jsonObj.keySet();
        Collection<Object> values = jsonObj.values();
        String upsertSql = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable
                + " (" + StringUtils.join(keys, ",")
                + ") values('"+StringUtils.join(values,"','")+"')";
        System.out.println("向phoenix表中插入数据的SQL:" + upsertSql);

        PhoenixUtil.executeSql(upsertSql);
        //将Redis中缓存的维度数据清除掉
        if("update".equals(type)){
            DimUtil.delCached(sinkTable,jsonObj.getString("id"));
        }

    }
}

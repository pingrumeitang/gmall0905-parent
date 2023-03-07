package com.atguigu.gmall.realtime.utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName,Tuple2.of("id",id));
    }
    /**
     * 查询维度数据优化：旁路缓存
     * 旁路缓存思路：先从缓存中获取维度数据，如果缓存中能够找到要关联的维度，直接将其作为返回值返回(缓存命中)；
     *          如果在缓存中没有找到要关联的维度，那么需要发送请求到Phoenix表中将维度查询出来，并将查询出来
     *          的维度放到缓存中进行缓存，方便下次使用。
     * 缓存技术选型
     *      状态：         性能好  维护性差
     *      redis：       性能不差   维护性好   √
     * 关于Redis的设置
     *      key：    dim:维度表表名:主键1_主键2
     *      type:String
     *      expire: 1day
     *      注意：如果维度数据发生了变化，需要从Redis中将缓存清除掉
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String> ... columnNameAndValues) {
        //拼接从Redis中查询数据的key
        StringBuilder redisKey = new StringBuilder("dim:"+tableName.toLowerCase()+":");
        //拼接查询语句
        StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        //遍历查询条件
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + " = '" + columnValue + "'");
            redisKey.append(columnValue);
            if(i < columnNameAndValues.length - 1){
                selectSql.append(" and ");
                redisKey.append("_");
            }
        }

        //操作redis的客户端
        Jedis jedis = null;
        //接收从Redis缓存中查询出来的数据
        String dimJsonStr = null;
        //用户封装方法的返回结果
        JSONObject dimJsonObj = null;

        try {
            // 先从缓存中获取维度数据
            jedis = RedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(StringUtils.isNotEmpty(dimJsonStr)){
            // 如果缓存中能够找到要关联的维度，直接将其作为返回值返回(缓存命中)
            dimJsonObj = JSON.parseObject(dimJsonStr);
        }else{
            // 如果在缓存中没有找到要关联的维度，那么需要发送请求到Phoenix表中将维度查询出来
            System.out.println("从Phoenix表中查询数据的SQL:" + selectSql);
            //底层还是调用PhoenixUtil工具类中的queryList方法
            List<JSONObject> dimJsonObjList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
            if (dimJsonObjList != null && dimJsonObjList.size() > 0) {
                //因为是根据维度的主键进行查询，查询结果只会有一条或者没有
                dimJsonObj = dimJsonObjList.get(0);
                // 将查询出来的维度放到缓存中进行缓存，方便下次使用。
                if(jedis != null){
                    jedis.setex(redisKey.toString(),3600*24,dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("从Phoenix表中没有找到对应的维度数据");
            }
        }
        if(jedis != null){
            System.out.println("~~~关闭Jedis客户端~~~");
            jedis.close();
        }

        return dimJsonObj;
    }
    //查询维度的方法
    public static JSONObject getDimInfoNoCached(String tableName, Tuple2<String, String> ... columnNameAndValues) {
        //拼接查询语句
        StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        //遍历查询条件
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + " = '" + columnValue + "'");
            if(i < columnNameAndValues.length - 1){
                selectSql.append(" and ");
            }
        }
        System.out.println("从Phoenix表中查询数据的SQL:" + selectSql);
        //底层还是调用PhoenixUtil工具类中的queryList方法
        List<JSONObject> dimJsonObjList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;
        if (dimJsonObjList != null && dimJsonObjList.size() > 0) {
            //因为是根据维度的主键进行查询，查询结果只会有一条或者没有
            dimJsonObj = dimJsonObjList.get(0);
        } else {
            System.out.println("从Phoenix表中没有找到对应的维度数据");
        }
        return dimJsonObj;
    }

    //从Redis缓存中删除数据
    public static void delCached(String tableName,String id){
        String redisKey = "dim:"+tableName.toLowerCase()+":" + id;
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(jedis != null){
                System.out.println("~~~删除缓存后，关闭Jedis客户端~~~");
                jedis.close();
            }
        }
    }

    public static void main(String[] args) {
        // System.out.println(getDimInfoNoCached("dim_base_trademark", Tuple2.of("id", "1"), Tuple2.of("tm_name", "三星10")));
        System.out.println(getDimInfo("dim_sku_info","1"));
    }
}

package com.atguigu.gmall.realtime.beans;
import com.alibaba.fastjson.JSONObject;
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimInfoJsonObj);

    String getKey(T obj);
}

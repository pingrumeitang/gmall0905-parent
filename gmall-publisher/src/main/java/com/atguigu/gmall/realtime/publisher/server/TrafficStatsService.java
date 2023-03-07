package com.atguigu.gmall.realtime.publisher.server;

import com.atguigu.gmall.realtime.publisher.beans.TrafficUvCt;

import java.util.List;

public interface TrafficStatsService {
    //获取某天渠道独立访客
    List<TrafficUvCt> getChUv(Integer date, Integer limit);
}

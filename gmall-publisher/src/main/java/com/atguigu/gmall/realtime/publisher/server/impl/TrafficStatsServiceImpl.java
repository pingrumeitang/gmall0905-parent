package com.atguigu.gmall.realtime.publisher.server.impl;

import com.atguigu.gmall.realtime.publisher.beans.TrafficUvCt;
import com.atguigu.gmall.realtime.publisher.mapper.TrafficStatsMapper;
import com.atguigu.gmall.realtime.publisher.server.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    TrafficStatsMapper trafficStatsMapper;
    @Override
    public List<TrafficUvCt> getChUv(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUv(date,limit);
    }
}

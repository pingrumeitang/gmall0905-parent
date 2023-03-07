package com.atguigu.gmall.realtime.publisher.server.impl;

import com.atguigu.gmall.realtime.publisher.mapper.TradeStatsMapper;
import com.atguigu.gmall.realtime.publisher.server.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
      TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGmv(date);
    }
}

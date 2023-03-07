package com.atguigu.gmall.realtime.publisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface TradeStatsMapper {
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(Integer date);
}

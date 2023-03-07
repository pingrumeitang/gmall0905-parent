package com.atguigu.gmall.realtime.publisher.server;

import java.math.BigDecimal;

public interface TradeStatsService {
    BigDecimal getGMV ( Integer date);
}

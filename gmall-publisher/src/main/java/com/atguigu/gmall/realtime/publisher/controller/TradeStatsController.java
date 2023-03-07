package com.atguigu.gmall.realtime.publisher.controller;

import com.atguigu.gmall.realtime.publisher.server.TradeStatsService;
import com.atguigu.gmall.realtime.publisher.utils.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
public class TradeStatsController {
    @Autowired
    TradeStatsService tradeStatsService;
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0){
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\"status\": 0,\"data\": " + gmv + "}";
        return json;
    }
}

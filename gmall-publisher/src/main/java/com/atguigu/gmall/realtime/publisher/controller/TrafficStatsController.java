package com.atguigu.gmall.realtime.publisher.controller;
import com.atguigu.gmall.realtime.publisher.beans.TrafficUvCt;
import com.atguigu.gmall.realtime.publisher.server.TrafficStatsService;
import com.atguigu.gmall.realtime.publisher.utils.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
@RestController
public class TrafficStatsController {
    @Autowired
    TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUv(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit
    ) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> uvCtList = trafficStatsService.getChUv(date,limit);
        List chList = new ArrayList();
        List ctList = new ArrayList();
        for (TrafficUvCt uvCt : uvCtList) {
            chList.add(uvCt.getCh());
            ctList.add(uvCt.getUvCt());
        }

        String json = "{\"status\": 0,\"data\": " +
                "{\"categories\": [\"" + StringUtils.join(chList, "\",\"") + "\"]," +
                "\"series\": [{\"name\": \"渠道\", \"data\": [" + StringUtils.join(ctList, ",") + "]}]}}";
        return json;
    }

}

package com.atguigu.gmall.realtime.publisher.utils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
public class DateFormatUtil {
    //获取当天日期
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}

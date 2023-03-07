package com.atguigu.gmall.realtime.publisher.mapper;

import com.atguigu.gmall.realtime.publisher.beans.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficStatsMapper {
    //获取某天不同渠道独立访客
    @Select("select ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window " +
            "where toYYYYMMDD(stt)=#{date} group by ch order by uv_ct desc limit #{limit}")
    List<TrafficUvCt> selectChUv(@Param("date") Integer date, @Param("limit") Integer limit);
}

package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.beans.KeywordBean;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * dws层关键字搜索
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) {
        //TODO 1.基本环境的准备
        //流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //注册表函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 2.设置检查点
        //TODO 3.从kafka的页面日志中读取数据并创建动态表,并设置水位线以及提取事件时间字段
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  common map<string,string>,\n" +
                "  page map<string,string>,\n" +
                "  ts BIGINT,\n" +
                "  row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND\n" +
                ") " + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log","dws_traffic_keyword_group"));
          /*tableEnv.executeSql("select * from page_log").print();*/
        //TODO 4.将搜索行为过滤出来,
        Table searchTable = tableEnv.sqlQuery("select \n" +
                " page['item'] fullword,\n" +
                " row_time\n" +
                "from page_log\n" +
                "where page['last_page_id']='search' and page['item_type']='keyword' and page['item'] is not null");
        tableEnv.createTemporaryView("search_table",searchTable);
        //tableEnv.executeSql("select * from search_table").print();
        //TODO 5.对搜索内容进行分词,并于原有表进行关联
        Table splitTable = tableEnv.sqlQuery("SELECT \n" +
                " keyword,row_time\n" +
                "FROM search_table,LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
        //tableEnv.executeSql("select * from split_table").print();
        //TODO 6.分组,开窗,进行计算
        Table reduceTable = tableEnv.sqlQuery("select\n" +
                " DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                " DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                " keyword,\n" +
                " count(*) keyword_count,\n" +
                " UNIX_TIMESTAMP()*1000 ts \n" +
                "from split_table group by keyword,TUMBLE(row_time, INTERVAL '10' second)");
        tableEnv.createTemporaryView("reduce_table",reduceTable);
        //tableEnv.executeSql("select * from reduce_table").print();

        //TODO 7.将动态表转化为流
        DataStream<KeywordBean> keywordDS = tableEnv.toDataStream(reduceTable, KeywordBean.class);
        keywordDS.print();
        //TODO 8.将流中的数据写到click house
        keywordDS.addSink(
                MyClickHouseUtil.getSinkFunction("insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)")
        );


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

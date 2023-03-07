package com.atguigu.gmall.realtime.test;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Test01_IntervalJoin {
    public static void main(String[] args) throws Exception{
        //TODO 1.基本环境的准备
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
        //TODO 2.检查点的设置
        //TODO 3.从指定的端口读取员工数据,封装为数据流,指定水位线为ts字段
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 9999)
                .map(r -> {
                    String[] split = r.split(",");
                    return new Emp(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]), Long.parseLong(split[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Emp>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Emp>() {
                    @Override
                    public long extractTimestamp(Emp emp, long l) {
                        return emp.getTs();
                    }
                }));
        //TODO 4.从指定的端口读取部门数据,封装为数据流,指定水位线为ts字段
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 9996)
                .map(r ->
                {
                    String[] split = r.split(",");
                    return new Dept(Integer.parseInt(split[0]), split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Dept>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                            @Override
                            public long extractTimestamp(Dept dept, long l) {
                                return dept.getTs();
                            }
                        }));

        //TODO 5.将两条数据流进行join并打印输出
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> joinDS = empDS.keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp emp, Dept dept, Context context, Collector<Tuple2<Emp, Dept>> collector) throws Exception {
                        collector.collect(Tuple2.of(emp, dept));
                    }
                });
      joinDS.print("xiuxiuxiu");

        env.execute();
    }
}

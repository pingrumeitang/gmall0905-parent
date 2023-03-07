package com.atguigu.gmall.realtime.app.func;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.DimJoinFunction;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    private ExecutorService executorService;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取线程池
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //开启线程
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        //根据流中的对象获取要关联的维度的主键
                        String key = getKey(obj);
                        //根据维度的主键获取维度对象
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        if(dimInfoJsonObj != null){
                            //将维度对象的属性补充流中对象上
                            join(obj,dimInfoJsonObj);
                        }
                        //将流中的对象向下游传递
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );

    }
}

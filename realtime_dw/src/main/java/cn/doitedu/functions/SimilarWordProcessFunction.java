package cn.doitedu.functions;

import cn.doitedu.beans.SearchAggBean;
import cn.doitedu.beans.SearchResultBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SimilarWordProcessFunction extends KeyedProcessFunction<String, SearchAggBean, SearchResultBean> {

    @Override
    public void open(Configuration parameters) throws Exception {

        // 构造http请求客户端

        // 申请一个状态(MapState)，用于存储已经查询过的  ：  搜索词 -> 分词，近义词

    }

    @Override
    public void processElement(SearchAggBean searchAggBean, KeyedProcessFunction<String, SearchAggBean, SearchResultBean>.Context context, Collector<SearchResultBean> collector) throws Exception {



        // 根据数据中的搜索词，构建 http请求体参数



        // 发出请求，得到响应



        // 从响应中提取我们要的结果数据



        // 输出结果


    }




}

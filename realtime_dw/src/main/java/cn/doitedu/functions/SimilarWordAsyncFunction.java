package cn.doitedu.functions;

import cn.doitedu.beans.SearchAggBean;
import cn.doitedu.beans.SearchResultBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SimilarWordAsyncFunction extends RichAsyncFunction<SearchAggBean, SearchResultBean> {
    CloseableHttpAsyncClient client;
    MapState<String, String> state;
    HttpPost post;
    SearchResultBean searchResultBean;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 构造 http请求客户端
        client = HttpAsyncClients.createDefault();


        // 构造一个post请求对象
        post = new HttpPost("http://doitedu:8081/api/post/simwords");
        // 请求头
        post.addHeader("Content-type", "application/json; charset=utf-8");
        post.addHeader("Accept", "application/json");

        // 申请一个状态(MapState)，用于存储已经查询过的  ：  搜索词 -> 分词，近义词
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("st", String.class, String.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.of(30, TimeUnit.MINUTES)).setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).build();
        descriptor.enableTimeToLive(ttlConfig);

        state = getRuntimeContext().getMapState(descriptor);


        // 构造一个用于输出结果的可重用的bean
        searchResultBean = new SearchResultBean();

    }

    @Override
    public void asyncInvoke(SearchAggBean searchAggBean, ResultFuture<SearchResultBean> resultFuture) throws Exception {

        // 取出搜索词，并封装成接口所要求的json结构
        String searchWord = searchAggBean.getKeyword();
        String words;
        String similarWord;

        String stateResult = state.get(searchWord);
        if (stateResult != null) {
            String[] split = stateResult.split("\001");
            words = split[0];
            similarWord = split[1];
        } else {
            HashMap<String, String> data = new HashMap<>();
            data.put("origin", searchWord);
            String jsonString = JSON.toJSONString(data);

            // 请求体
            post.setEntity(new StringEntity(jsonString, StandardCharsets.UTF_8));

            // 发出请求，得到响应
            Future<HttpResponse> responseFuture = client.execute(post, null);
            // HttpResponse response = responseFuture.get();  // 同步阻塞方法

            CompletableFuture.supplyAsync(new Supplier<SearchResultBean>() {
                        @Override
                        public SearchResultBean get() {

                            try {
                                HttpResponse response = responseFuture.get();

                                // 从response中提取响应体（服务器返回的json）
                                HttpEntity entity = response.getEntity();
                                String responseJson = EntityUtils.toString(entity);

                                // 从响应中提取我们要的结果数据(近义词  分词）
                                JSONObject jsonObject = JSON.parseObject(responseJson);
                                String words = jsonObject.getString("words");
                                String similarWord = jsonObject.getString("similarWord");

                                // 将请求到的结果放入状态缓存起来
                                state.put(searchWord, words + "\001" + similarWord);

                                searchResultBean.set(searchAggBean.getUser_id(),
                                        searchAggBean.getKeyword(),
                                        words,
                                        similarWord,
                                        searchAggBean.getSearch_id(),
                                        searchAggBean.getSearch_time(),
                                        searchAggBean.getReturn_item_count(),
                                        searchAggBean.getClick_item_count()
                                );

                                return searchResultBean;

                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }


                        }
                    })
                    .thenAccept(new Consumer<SearchResultBean>() {
                        @Override
                        public void accept(SearchResultBean searchResultBean) {
                            resultFuture.complete(Collections.singleton(searchResultBean));
                        }
                    });


        }

        // 返回结果
        resultFuture.complete(Collections.singleton(searchResultBean));

    }
}

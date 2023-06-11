package cn.doitedu.dashboard;

import cn.doitedu.beans.*;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/10
 * @Desc: 学大数据，上多易教育
 * <p>
 * 实时看板指标： 每小时 ，每个品牌中， 已支付金额最大的前 N个商品
 **/
@Slf4j
public class Job6_BrandTopnPayedProduct_API2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource1 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> orderCdcStream = env.fromSource(mySqlSource1, WatermarkStrategy.noWatermarks(), "cdc");
        SingleOutputStreamOperator<OrderCdcOuterBean> orderOuterBeanStream = orderCdcStream.map(json -> JSON.parseObject(json, OrderCdcOuterBean.class));
        // 广播出来
        MapStateDescriptor<Long, OrderCdcInnerBean> desc = new MapStateDescriptor<>("order-bc", Long.class, OrderCdcInnerBean.class);
        BroadcastStream<OrderCdcOuterBean> broadcast = orderOuterBeanStream.broadcast(desc);

        //orderOuterBeanStream.print();


        MySqlSource<String> mySqlSource2 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order_item") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> itemCdcStream = env.fromSource(mySqlSource2, WatermarkStrategy.noWatermarks(), "cdc");
        SingleOutputStreamOperator<ItemCdcOuterBean> itemOuterBeanStream = itemCdcStream.map(json -> JSON.parseObject(json, ItemCdcOuterBean.class));

        //itemOuterBeanStream.print();


        SingleOutputStreamOperator<BrandTopnBean> res = itemOuterBeanStream
                .map(outer -> outer.getAfter())
                .keyBy(bean -> bean.getProduct_brand())
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcOuterBean, BrandTopnBean>() {
                    MapState<Long, ItemCdcInnerBean> itemsState;
                    ValueState<Long> timerState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // id -> itemid,价格*数量
                        itemsState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ItemCdcInnerBean>("p-amt", Long.class, ItemCdcInnerBean.class));
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));

                    }

                    @Override
                    public void processElement(ItemCdcInnerBean itemBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcOuterBean, BrandTopnBean>.ReadOnlyContext readOnlyContext, Collector<BrandTopnBean> collector) throws Exception {

                        // 初始定时器注册
                        Long timerTime = timerState.value();
                        if (timerTime == null) {
                            timerTime = (readOnlyContext.currentProcessingTime()/60000)*60000 + 60*1000;
                            readOnlyContext.timerService().registerProcessingTimeTimer(timerTime);
                            timerState.update(timerTime);
                        }

                        // 新增或覆盖
                        long id = itemBean.getId();
                        itemsState.put(id, itemBean);
                    }

                    @Override
                    public void processBroadcastElement(OrderCdcOuterBean orderCdcOuterBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcOuterBean, BrandTopnBean>.Context context, Collector<BrandTopnBean> collector) throws Exception {

                        BroadcastState<Long, OrderCdcInnerBean> broadcastState = context.getBroadcastState(desc);

                        OrderCdcInnerBean orderBean = orderCdcOuterBean.getAfter();


                        // 将收到的订单主表数据，放入广播状态
                        long orderId = orderBean.getId();
                        broadcastState.put(orderId, orderBean);

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcOuterBean, BrandTopnBean>.OnTimerContext ctx, Collector<BrandTopnBean> out) throws Exception {

                        long stat_start = timestamp - 60*1000;
                        long stat_end = timestamp;

                        log.warn("---------------------------------");
                        ReadOnlyBroadcastState<Long, OrderCdcInnerBean> broadcastState = ctx.getBroadcastState(desc);

                        //  itemId->金额  ,用于对每个商品聚合总支付额
                        HashMap<Long, BigDecimal> aggMap = new HashMap<>();

                        // 遍历items状态中的每一条商品购买记录，查询它所属的订单是否是支付状态
                        Iterable<Map.Entry<Long, ItemCdcInnerBean>> entries = itemsState.entries();
                        for (Map.Entry<Long, ItemCdcInnerBean> entry : entries) {

                            ItemCdcInnerBean itemBean = entry.getValue();

                            long productId = itemBean.getProduct_id();
                            BigDecimal price = itemBean.getProduct_price();
                            int quantity = itemBean.getProduct_quantity();
                            long orderId = itemBean.getOrder_id();

                            // 从订单主数据的广播状态中，查询该订单的支付状态
                            OrderCdcInnerBean orderBean = broadcastState.get(orderId);
                            int status = orderBean.getStatus();
                            // 查询订单的支付时间
                            long paymentTime = orderBean.getPayment_time() - 8*60*60*1000;

                            // 如果订单是已支付,且订单支付时间在本小时区间
                            if ( (status == 1 || status==2 || status == 3) &&  paymentTime >= stat_start && paymentTime< stat_end) {
                                // 对 item的总额累加
                                BigDecimal oldAmt = aggMap.get(productId);
                                if (oldAmt == null) {
                                    oldAmt = BigDecimal.ZERO;
                                }
                                aggMap.put(productId, oldAmt.add(price.multiply(BigDecimal.valueOf(quantity))));
                            }
                        }


                        for (Map.Entry<Long, BigDecimal> entry : aggMap.entrySet()) {
                            System.out.println(new BrandTopnBean(ctx.getCurrentKey(), entry.getKey(), entry.getValue(), stat_start, stat_end));
                        }


                        Long newTimerTime = timerState.value() + 60*1000;
                        timerState.update(newTimerTime);
                        ctx.timerService().registerProcessingTimeTimer(newTimerTime);

                    }
                });


        env.execute();


    }


    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SortBean implements Comparable<SortBean> {
        private String brand;
        private long itemId;
        private BigDecimal amt;

        @Override
        public int compareTo(SortBean o) {
            return this.amt.compareTo(o.amt);
        }
    }

}

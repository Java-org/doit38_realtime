package cn.doitedu.test;

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
        SingleOutputStreamOperator<OrderCdcRecord> orderOuterBeanStream = orderCdcStream.map(json -> JSON.parseObject(json, OrderCdcRecord.class));
        // 广播出来
        MapStateDescriptor<Long, OrderCdcData> desc = new MapStateDescriptor<>("order-bc", Long.class, OrderCdcData.class);
        BroadcastStream<OrderCdcRecord> broadcast = orderOuterBeanStream.broadcast(desc);


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

        SingleOutputStreamOperator<SortBean> res = itemOuterBeanStream
                .map(outer -> outer.getAfter())
                .keyBy(bean -> bean.getProduct_brand())
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcRecord, SortBean>() {
                    MapState<Long, ItemCdcInnerBean> itemsState;
                    ValueState<Long> timerState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // id -> itemid,价格*数量
                        itemsState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ItemCdcInnerBean>("p-amt", Long.class, ItemCdcInnerBean.class));

                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));

                    }

                    @Override
                    public void processElement(ItemCdcInnerBean itemBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcRecord, SortBean>.ReadOnlyContext readOnlyContext, Collector<SortBean> collector) throws Exception {

                        Long timerTime = timerState.value();
                        if (timerTime == null) {
                            timerTime = readOnlyContext.currentProcessingTime() + 5000;
                            readOnlyContext.timerService().registerProcessingTimeTimer(timerTime);
                            timerState.update(timerTime);
                        }


                        int quantity = itemBean.getProduct_quantity();
                        long id = itemBean.getProduct_id();
                        BigDecimal price = itemBean.getProduct_price();
                        long orderId = itemBean.getOrder_id();
                        BigDecimal amt = price.multiply(BigDecimal.valueOf(quantity));

                        // 新增或覆盖
                        itemsState.put(id, itemBean);
                    }

                    @Override
                    public void processBroadcastElement(OrderCdcRecord orderCdcOuterBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcRecord, SortBean>.Context context, Collector<SortBean> collector) throws Exception {


                        BroadcastState<Long, OrderCdcData> broadcastState = context.getBroadcastState(desc);

                        OrderCdcData orderBean = orderCdcOuterBean.getAfter();
                        long modifyTime = orderBean.getModify_time();
                        long orderId = orderBean.getId();

                        OrderCdcData oldOrderBean = broadcastState.get(orderId);

                        if (oldOrderBean != null && oldOrderBean.getModify_time() > modifyTime) {

                        } else {
                            broadcastState.put(orderId, orderBean);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcRecord, SortBean>.OnTimerContext ctx, Collector<SortBean> out) throws Exception {

                        log.warn("---------------------------------");
                        ReadOnlyBroadcastState<Long, OrderCdcData> broadcastState = ctx.getBroadcastState(desc);

                        //  itemId->金额
                        HashMap<Long, BigDecimal> aggMap = new HashMap<>();
                        // Bean(itemId,金额) ->null
                        TreeMap<SortBean, String> sortMap = new TreeMap<>();

                        // 遍历items状态中的每一条记录，查询它所属的订单是否是支付状态
                        Iterable<Map.Entry<Long, ItemCdcInnerBean>> entries = itemsState.entries();
                        for (Map.Entry<Long, ItemCdcInnerBean> entry : entries) {

                            ItemCdcInnerBean itemBean = entry.getValue();
                            long productId = itemBean.getProduct_id();
                            BigDecimal price = itemBean.getProduct_price();
                            int quantity = itemBean.getProduct_quantity();
                            long orderId = itemBean.getOrder_id();

                            OrderCdcData orderBean = broadcastState.get(orderId);
                            int status = orderBean.getStatus();
                            long paymentTime = orderBean.getPayment_time();

                            // 如果订单是已支付,且订单支付时间在本小时区间
                            if (status > 0 && paymentTime > 0) {
                                // 对item的总额累加
                                BigDecimal oldAmt = aggMap.get(productId);
                                if (oldAmt == null) {
                                    oldAmt = BigDecimal.ZERO;
                                }
                                aggMap.put(productId, oldAmt.add(price.multiply(BigDecimal.valueOf(quantity))));
                            }
                        }


                        // 遍历aggMap，放入sortMap排序
                        for (Map.Entry<Long, BigDecimal> entry : aggMap.entrySet()) {
                            Long itemId = entry.getKey();
                            BigDecimal amt = entry.getValue();

                            SortBean sortBean = new SortBean(ctx.getCurrentKey(), itemId, amt);
                            sortMap.put(sortBean, null);
                            if (sortMap.size() > 2) {
                                sortMap.pollLastEntry();
                            }
                        }

                        // 输出结果
                        for (Map.Entry<SortBean, String> entry : sortMap.entrySet()) {
                            out.collect(entry.getKey());
                        }

                        Long newTimerTime = timerState.value()+5000;
                        timerState.update(newTimerTime);
                        ctx.timerService().registerProcessingTimeTimer( newTimerTime);


                    }
                });

        res.print();


        env.execute();


    }


    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SortBean implements Comparable<SortBean>{
        private String brand;
        private long itemId;
        private BigDecimal amt;

        @Override
        public int compareTo(SortBean o) {
            return this.amt.compareTo(o.amt);
        }
    }

}

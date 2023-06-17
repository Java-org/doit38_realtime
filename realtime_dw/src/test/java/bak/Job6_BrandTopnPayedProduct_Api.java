package bak;

import cn.doitedu.beans.*;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
public class Job6_BrandTopnPayedProduct_Api {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);

        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        /* *
         * 1. 订单主表 数据读取及处理
         */
        MySqlSource<String> mySqlSource1 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        // 读取订单主表数据
        DataStreamSource<String> orderCdcStream = env.fromSource(mySqlSource1, WatermarkStrategy.noWatermarks(), "order-cdc");

        // 将订单主表数据解析，并只保留after数据，并封装成JavaBean
        SingleOutputStreamOperator<OrderCdcData> orderOuterBeanStream =
                orderCdcStream.map(json -> JSON.parseObject(json, OrderCdcRecord.class).getAfter());

        // 广播订单主表数据
        MapStateDescriptor<Long, OrderCdcData> desc = new MapStateDescriptor<>("order-bc", Long.class, OrderCdcData.class);
        BroadcastStream<OrderCdcData> broadcast = orderOuterBeanStream.broadcast(desc);


        /* *
         * 2. 订单商品明细表 数据读取及处理
         */
        MySqlSource<String> mySqlSource2 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order_item") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> itemCdcStream = env.fromSource(mySqlSource2, WatermarkStrategy.noWatermarks(), "item-cdc");
        SingleOutputStreamOperator<ItemCdcOuterBean> itemOuterBeanStream = itemCdcStream.map(json -> JSON.parseObject(json, ItemCdcOuterBean.class));

        SingleOutputStreamOperator<BrandTopnBean> res =
                itemOuterBeanStream
                        .map(ItemCdcOuterBean::getAfter)  // 只取cdc数据中的after
                        .keyBy(ItemCdcInnerBean::getProduct_brand)  // 按品牌分区
                        .connect(broadcast)  // 连接广播数据（订单主表数据）
                        .process(new KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcData, BrandTopnBean>() {
                            MapState<Long, ItemCdcInnerBean> itemsState;
                            ValueState<Long> timerState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 用于存储item记录明细的状态：  id -> [itemBean] ，如果有 op=u 的操作，则根据主键id直接覆盖以保留最新数据
                                itemsState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ItemCdcInnerBean>("p-amt", Long.class, ItemCdcInnerBean.class));

                                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));

                            }

                            @Override
                            public void processElement(ItemCdcInnerBean itemBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcData, BrandTopnBean>.ReadOnlyContext readOnlyContext, Collector<BrandTopnBean> collector) throws Exception {

                                // 初始定时器注册
                                Long timerTime = timerState.value();
                                if (timerTime == null) {
                                    timerTime = (readOnlyContext.currentProcessingTime() / 60000) * 60000 + 60 * 1000;
                                    readOnlyContext.timerService().registerProcessingTimeTimer(timerTime);
                                    timerState.update(timerTime);
                                }

                                // 将收到的item记录，存入状态，新增或覆盖
                                long id = itemBean.getId();
                                itemsState.put(id, itemBean);
                            }

                            @Override
                            public void processBroadcastElement(OrderCdcData orderBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcData, BrandTopnBean>.Context context, Collector<BrandTopnBean> collector) throws Exception {

                                BroadcastState<Long, OrderCdcData> broadcastState = context.getBroadcastState(desc);

                                // 将收到的订单主表数据，放入广播状态 (新增或覆盖：只保留最新的数据状态）
                                long orderId = orderBean.getId();
                                broadcastState.put(orderId, orderBean);

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcData, BrandTopnBean>.OnTimerContext ctx, Collector<BrandTopnBean> out) throws Exception {
                                // 算出本次触发所统计的 时间窗口范围
                                long stat_start = timestamp - 60 * 1000;
                                long stat_end = timestamp;

                                // 获得存储订单主表数据的 广播状态
                                ReadOnlyBroadcastState<Long, OrderCdcData> broadcastState = ctx.getBroadcastState(desc);

                                // 构造一个临时 hashmap ：[商品Id|商品名 -> 支付总额] ,用于对每个商品聚合总支付额
                                HashMap<String, BigDecimal> aggMap = new HashMap<>();

                                // 遍历items状态中的每一条商品购买记录，查询它所属的订单是否是支付状态
                                Iterable<Map.Entry<Long, ItemCdcInnerBean>> entries = itemsState.entries();
                                for (Map.Entry<Long, ItemCdcInnerBean> entry : entries) {
                                    // 取出一条item数据
                                    ItemCdcInnerBean itemBean = entry.getValue();

                                    // 取出item数据的各个字段
                                    long id = itemBean.getId();
                                    long productId = itemBean.getProduct_id();
                                    String productName = itemBean.getProduct_name();
                                    BigDecimal price = itemBean.getProduct_price();
                                    int quantity = itemBean.getProduct_quantity();
                                    long orderId = itemBean.getOrder_id();

                                    // 从广播状态取出一条 订单主表 数据
                                    OrderCdcData orderBean = broadcastState.get(orderId);
                                    // 取到这条订单的支付状态
                                    int status = orderBean.getStatus();
                                    // 取到这条订单的支付时间 （由于mysql的时区问题，这里要-8个小时）
                                    long paymentTime = orderBean.getPayment_time() - 8 * 60 * 60 * 1000L;

                                    // 如果订单是已支付,且订单支付时间在本小时区间
                                    if ((status == 1 || status == 2 || status == 3) && paymentTime >= stat_start && paymentTime < stat_end) {
                                        // 取出该商品的支付额历史累加值
                                        BigDecimal oldAmt = aggMap.get(productId + "\001" + productName);
                                        if (oldAmt == null) {
                                            oldAmt = BigDecimal.ZERO;
                                        }

                                        // 将该商品的已累计支付额 加上 本条数据的 支付额，放入聚合map
                                        aggMap.put(productId + "\001" + productName, oldAmt.add(price.multiply(BigDecimal.valueOf(quantity))));
                                    }
                                }


                                // 构造一个TreeMap用于排序求topn
                                TreeMap<BrandTopnBean, String> sortMap = new TreeMap<>();

                                // 遍历金额聚合map
                                for (Map.Entry<String, BigDecimal> entry : aggMap.entrySet()) {
                                    // map中的 key是 : "商品id\001商品名称"
                                    String[] pidAndName = entry.getKey().split("\001");
                                    long pid = Long.parseLong(pidAndName[0]);
                                    String pName = pidAndName[1];

                                    BrandTopnBean brandTopnBean = new BrandTopnBean(ctx.getCurrentKey(), pid, pName, entry.getValue(), stat_start, stat_end);

                                    // 将aggMap取出的这条数据，放入TreeMap中自动排序
                                    sortMap.put(brandTopnBean,null);
                                    // 如果treeMap中的数据条数已经大于topn个，则移除最后一个
                                    if(sortMap.size()>2){
                                        sortMap.pollLastEntry();
                                    }
                                }


                                // 输出topn结果
                                for (Map.Entry<BrandTopnBean, String> entry : sortMap.entrySet()) {
                                    out.collect(entry.getKey());
                                }

                                // 注册下一轮触发定时器
                                long newTimerTime = timerState.value() + 60 * 1000;
                                timerState.update(newTimerTime);
                                ctx.timerService().registerProcessingTimeTimer(newTimerTime);
                            }
                        });

        // 将结果写入mysql
        SinkFunction<BrandTopnBean> sink = JdbcSink.sink(
                //"insert into brand_payed_topn_item_m (dash_name, time_start, time_end,product_brand,product_id,product_name,product_amount) values (?,?, ?,?,?,?,?) on duplicate key update dash_name='topn看板' ",
                "insert into brand_payed_topn_item_m (dash_name, time_start, time_end,product_brand,product_id,product_name,product_amount) values (?,?, ?,?,?,?,?)  ",
                (statement, bean) -> {
                    statement.setString(1,"topn看板");
                    statement.setLong(2,bean.getStatic_start_time());
                    statement.setLong(3,bean.getStatic_end_time());
                    statement.setString(4,bean.getBrand());
                    statement.setLong(5,bean.getProduct_id());
                    statement.setString(6,bean.getProduct_name());
                    statement.setBigDecimal(7, bean.getProduct_amt());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doitedu:3306/realtimedw")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );
        res.addSink(sink);
        env.execute();
    }

}

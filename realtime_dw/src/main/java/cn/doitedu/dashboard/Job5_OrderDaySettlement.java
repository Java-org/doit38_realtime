package cn.doitedu.dashboard;

import cn.doitedu.beans.OrderCdcInnerBean;
import cn.doitedu.beans.OrderCdcOuterBean;
import cn.doitedu.beans.OrderDiffValues;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/8
 * @Desc: 学大数据，上多易教育
 * <p>
 * 订单日清日结看板指标
 **/
@Slf4j
public class Job5_OrderDaySettlement {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/rtdw/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1.创建一个cdc连接器的source算子，用于读取 mysql中订单表 binlog

        // 用于指定 JsonDebeziumDeserializationSchema在对decimal类型进行反序列化时的返回格式（数字格式）
        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc");

        /*
            订单总数、总额 、应付总额 （当日新订单）
            待支付订单数、订单额  （当日新订单）
            支付订单数、订单额  （当日支付）
            发货订单数、订单额  （当日发货）
            完成订单数、订单额  （当日确认）
         */
        SingleOutputStreamOperator<OrderCdcOuterBean> beanStream
                = cdcStream.map(json -> JSON.parseObject(json, OrderCdcOuterBean.class));

        /**
         * 计算整体方案
         *   1. 因为上述指标都是全局聚合值，所有一定需要一个最终的单并行度算子
         *   2. 但也不能整个程序就一个单并行task来处理所有数据和逻辑，我们设计了一个2层的计算方案
         *     2.1  keyBy(order_id) 后，对同一个订单的 changelog 数据按照需求逻辑输出：调整值
         *     2.2  全局单并行：把上游输入过来的持续不断的 “调整值“ 进行累加
         */
        SingleOutputStreamOperator<OrderDiffValues> valueChangeStream = beanStream.keyBy(outer -> outer.getAfter().getId())
                .process(new KeyedProcessFunction<Long, OrderCdcOuterBean, OrderDiffValues>() {
                    @Override
                    public void processElement(OrderCdcOuterBean orderCdcOuterBean, KeyedProcessFunction<Long, OrderCdcOuterBean, OrderDiffValues>.Context context, Collector<OrderDiffValues> collector) throws Exception {
                        OrderCdcInnerBean before = orderCdcOuterBean.getBefore();
                        OrderCdcInnerBean after = orderCdcOuterBean.getAfter();
                        String op = orderCdcOuterBean.getOp();

                        // 先构造一个初始值全为0的调整结果bean
                        OrderDiffValues orderDiffValues = new OrderDiffValues();

                        // 订单总数调整
                        long truncate = 24 * 60 * 60 * 1000L;
                        // 如果 before 无效，而现在有效，则订单数要 +1
                        if (after.getCreate_time() >= (new Date().getTime() / truncate) * truncate) {

                            if ((before == null || before.getStatus() >= 4) && after.getStatus() <= 3) {
                                // 调整数量,+:
                                orderDiffValues.setTotalCount(1);

                                // 调整 +:  原价 总金额
                                orderDiffValues.setTotalOriginAmount(after.getTotal_amount());
                            }
                            // 如果 before有效，而现在无效，则订单数要 -1
                            else if ((before != null && before.getStatus() <= 3) && after.getStatus() >= 4) {

                                // 调整数量   -:
                                orderDiffValues.setTotalCount(-1);

                                // 调整原价 总金额  -:
                                orderDiffValues.setTotalOriginAmount(before.getTotal_amount().negate());
                            }

                            // before无效，现在无效  ^   before有效，现在有效
                            else {

                                // 调整数量 0:不变
                                orderDiffValues.setTotalCount(0);

                                // 调整金额 ，有效 -> 有效 ，调整额度： 新值-旧值
                                if (before != null && before.getStatus() <= 3 && after.getStatus() <= 3) {
                                    // 调整为：  现在的金额 - 此前的金额
                                    orderDiffValues.setTotalOriginAmount(after.getTotal_amount().subtract(before.getTotal_amount()));

                                }

                            }
                        }


                        // 订单实付总额调整


                        // 待支付订单数调整


                        // 待支付订单额调整


                        // 支付订单数调整


                        // 支付订单额调整


                        // 发货订单数调整


                        // 发货订单额调整


                        // 完成订单数调整


                        // 完成订单额调整


                        collector.collect(orderDiffValues);

                    }
                });


        SingleOutputStreamOperator<OrderDiffValues> cumulate =
                valueChangeStream
                        .keyBy(diff -> 1)
                        .process(new KeyedProcessFunction<Integer, OrderDiffValues, OrderDiffValues>() {
                            ValueState<OrderDiffValues> cumulateState;
                            ValueState<Long> timerState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                cumulateState = getRuntimeContext().getState(new ValueStateDescriptor<OrderDiffValues>("cumulate", OrderDiffValues.class));
                                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                            }

                            @Override
                            public void processElement(OrderDiffValues orderDiffValues, KeyedProcessFunction<Integer, OrderDiffValues, OrderDiffValues>.Context context, Collector<OrderDiffValues> collector) throws Exception {

                                // 首次收到数据，注册一个用于定时输出结果的定时器
                                if (timerState.value() == null) {
                                    long timerTime = ((context.timerService().currentProcessingTime() + 10000L)/60000L)*60000L;
                                    timerState.update(timerTime);
                                    context.timerService().registerProcessingTimeTimer(timerTime);
                                    log.warn("注册了定时器 : {}", Timestamp.from(Instant.ofEpochMilli(timerTime)));
                                }

                                // 取出此前汇总的老结果
                                OrderDiffValues oldCumulateValues = cumulateState.value();

                                // 如果状态中的老结果尚为空，则本次收到的汇总数据存入状态
                                if (oldCumulateValues == null) {
                                    cumulateState.update(orderDiffValues);
                                } else {
                                    // 否则，将此次收到的各个调整值，跟此前累积的值进行合并
                                    merge(oldCumulateValues, orderDiffValues);
                                }


                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, OrderDiffValues, OrderDiffValues>.OnTimerContext ctx, Collector<OrderDiffValues> collector) throws Exception {

                                log.warn("定时器被调用 : {}", Timestamp.from(Instant.ofEpochMilli(timestamp)));
                                log.warn("并注册新定时 : {}", Timestamp.from(Instant.ofEpochMilli(timerState.value() + 10 * 1000L)));

                                // 输出此刻的汇总结果
                                OrderDiffValues result = cumulateState.value();
                                result.setOutTime(Timestamp.from(Instant.ofEpochMilli(timestamp)));
                                collector.collect(result);

                                // 注册下一次输出数据的定时器
                                long newTime = timerState.value() + 10 * 1000L;
                                ctx.timerService().registerProcessingTimeTimer(newTime);
                                timerState.update(newTime);

                            }
                        });

        //cumulate.print();

        // 构造一个mysql的jdbc sink function
        SinkFunction<OrderDiffValues> sink = JdbcSink.sink(
                "insert into order_day_settlement (update_time, total_origin_amount, total_count) values (?, ?, ?)",
                (statement, orderDiffValues) -> {
                    statement.setTimestamp(1,orderDiffValues.getOutTime());
                    statement.setBigDecimal(2, orderDiffValues.getTotalOriginAmount());
                    statement.setInt(3, orderDiffValues.getTotalCount());
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

        // 输出到  sink
        cumulate.addSink(sink);

        env.execute();

    }


    /**
     * 新旧汇总数据  合并逻辑工具方法
     * @param old
     * @param cur
     */
    public static void merge(OrderDiffValues old, OrderDiffValues cur) {

        old.setTotalCount(old.getTotalCount() + cur.getTotalCount());
        old.setTotalOriginAmount(old.getTotalOriginAmount().add(cur.getTotalOriginAmount()));
        old.setTotalRealAmount(old.getTotalRealAmount().add(cur.getTotalRealAmount()));

        old.setToPayTotalCount(old.getToPayTotalCount() + cur.getToPayTotalCount());
        old.setToPayTotalAmount(old.getToPayTotalAmount().add(cur.getToPayTotalAmount()));

        old.setPayedTotalCount(old.getPayedTotalCount() + cur.getPayedTotalCount());
        old.setPayedTotalAmount(old.getPayedTotalAmount().add(cur.getPayedTotalAmount()));

        old.setDeliveredTotalCount(old.getDeliveredTotalCount() + cur.getDeliveredTotalCount());
        old.setDeliveredTotalAmount(old.getDeliveredTotalAmount().add(cur.getDeliveredTotalAmount()));

        old.setCompletedTotalCount(old.getCompletedTotalCount() + cur.getCompletedTotalCount());
        old.setCompletedTotalAmount(old.getCompletedTotalAmount().add(cur.getCompletedTotalAmount()));

    }


}

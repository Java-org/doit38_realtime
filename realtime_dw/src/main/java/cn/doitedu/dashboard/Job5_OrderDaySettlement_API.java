package cn.doitedu.dashboard;

import cn.doitedu.beans.OrderCdcRecord;
import cn.doitedu.beans.OrderDiffValues;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/17
 * @Desc: 学大数据，上多易教育
 * <p>
 * 订单日清日结看板，用api来实现的版本
 **/
public class Job5_OrderDaySettlement_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);


        // 用于指定 JsonDebeziumDeserializationSchema在对decimal类型进行反序列化时的返回格式（数字格式）
        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("rtmk") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("rtmk.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "order-source");
        SingleOutputStreamOperator<OrderCdcRecord> recordStream = sourceStream.map(json -> JSON.parseObject(json, OrderCdcRecord.class));

        KeyedStream<OrderCdcRecord, Long> keyedStream = recordStream.keyBy(bean -> {
            return bean.getBefore() == null ? bean.getAfter().getId() : bean.getBefore().getId();
        });

        // 核心逻辑
        SingleOutputStreamOperator<OrderDiffValues> diffValues = keyedStream.process(new KeyedProcessFunction<Long, OrderCdcRecord, OrderDiffValues>() {
            OrderDiffValues orderDiffValues;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderDiffValues = new OrderDiffValues();
            }

            @Override
            public void processElement(OrderCdcRecord record, KeyedProcessFunction<Long, OrderCdcRecord, OrderDiffValues>.Context context, Collector<OrderDiffValues> collector) throws Exception {

                // 对差值封装bean清零
                orderDiffValues.clear();

                // 根据收到的数据的状态变化，输出  订单数、订单额的调整值
                String orderCreateDateStr = DateFormatUtils.format(record.getAfter().getCreate_time().getTime(), "yyyy-MM-dd");
                String currentDateStr = DateFormatUtils.format(new Date().getTime(), "yyyy-MM-dd");

                /* *
                 * 一、今日 订单总数、总额、应付总额的 调整
                 */
                // 订单从 无效-> 有效  : 金额要调整 + , 数量要调整 +
                if (orderCreateDateStr.equals(currentDateStr)) {
                    if (record.getBefore() == null || (record.getBefore().getStatus() > 3 && record.getAfter().getStatus() <= 3)) {
                        orderDiffValues.setTotalCount(1);
                        orderDiffValues.setTotalOriginAmount(record.getAfter().getTotal_amount());
                        orderDiffValues.setTotalRealAmount(record.getAfter().getPay_amount());
                    }
                    // 订单从 有效-> 无效 : 金额要调整 - , 数量要调整 -
                    if (record.getBefore() != null && record.getBefore().getStatus() <= 3 && record.getAfter().getStatus() > 3) {
                        orderDiffValues.setTotalCount(-1);
                        orderDiffValues.setTotalOriginAmount(record.getBefore().getTotal_amount().negate());
                        orderDiffValues.setTotalRealAmount(record.getBefore().getPay_amount().negate());
                    }

                    // 订单从 有效-> 有效 : 金额要调整 （after-before)
                    if (record.getBefore() != null && record.getBefore().getStatus() <= 3 && record.getAfter().getStatus() <= 3) {
                        orderDiffValues.setTotalOriginAmount(record.getAfter().getTotal_amount().subtract(record.getBefore().getTotal_amount()));
                        orderDiffValues.setTotalRealAmount(record.getAfter().getPay_amount().subtract(record.getBefore().getPay_amount()));
                    }

                    // 订单从 无效-> 无效 : 不需要任何处理

                    orderDiffValues.setMetricFlag("a");
                    collector.collect(orderDiffValues);
                }


                /* *
                 * 二、待支付订单数、额的调整
                 */


                /* *
                 * 三、已支付订单数、额的调整
                 */




                /* *
                 * 四、已发货订单数、额的调整
                 */



                /* *
                 * 五、已确认订单数、额的调整
                 */


            }
        });

        // 对上游的差值，进行汇总
        // 这里是一个全局汇总，我们应该用一个并行度去做
        // 如果公司的订单相关数量真的很大，也可以用多个并行度：每一类指标，发给一个并行度去处理

        SingleOutputStreamOperator<OrderDiffValues> resultStream = diffValues.keyBy(OrderDiffValues::getMetricFlag)
                .process(new KeyedProcessFunction<String, OrderDiffValues, OrderDiffValues>() {
                    ValueState<OrderDiffValues> cumulateState;
                    ValueState<Long> timerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        cumulateState = getRuntimeContext().getState(new ValueStateDescriptor<OrderDiffValues>("cumulate-state", OrderDiffValues.class));

                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
                    }

                    @Override
                    public void processElement(OrderDiffValues orderDiffValues, KeyedProcessFunction<String, OrderDiffValues, OrderDiffValues>.Context context, Collector<OrderDiffValues> collector) throws Exception {

                        // 注册首次定时器时间
                        if (timerState.value() == null) {
                            long timerTime = (context.timerService().currentProcessingTime() / 60000L) * 60000L + 60000L;
                            context.timerService().registerProcessingTimeTimer(timerTime);
                            timerState.update(timerTime);
                        }

                        OrderDiffValues oldValues = cumulateState.value();
                        if (oldValues == null) {
                            oldValues = orderDiffValues;
                            cumulateState.update(oldValues);
                        } else {
                            oldValues.setTotalCount(oldValues.getTotalCount() + orderDiffValues.getTotalCount());
                            oldValues.setTotalOriginAmount(oldValues.getTotalOriginAmount().add(orderDiffValues.getTotalOriginAmount()));
                            oldValues.setTotalRealAmount(oldValues.getTotalRealAmount().add(orderDiffValues.getTotalRealAmount()));
                        }

                        // 如果在这里输出，那就是时效性最高的： 源表中发生任意变化，结果立马更新
                        // collector.collect(oldValues);

                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, OrderDiffValues, OrderDiffValues>.OnTimerContext ctx, Collector<OrderDiffValues> out) throws Exception {
                        OrderDiffValues value = cumulateState.value();
                        value.setOutTime(new Timestamp(timestamp));
                        out.collect(value);

                        // 注册下一次输出的定时器时间
                        ctx.timerService().registerProcessingTimeTimer(timestamp + 60000L);
                    }

                });

        // 构造jdbc sink
        SinkFunction<OrderDiffValues> sink = JdbcSink.sink(
                "insert into day_order_settlement (update_time, today_order_total_count, today_order_total_amount, today_order_total_real_amount) values (?, ?, ?, ?)",
                (statement, values) -> {
                    statement.setTimestamp(1, values.getOutTime());
                    statement.setLong(2, values.getTotalCount());
                    statement.setBigDecimal(3, values.getTotalOriginAmount());
                    statement.setBigDecimal(4, values.getTotalRealAmount());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doitedu:3306/rtmk")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );


        resultStream.addSink(sink);
        env.execute();
    }
}

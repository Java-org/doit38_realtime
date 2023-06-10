package cn.doitedu.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Order {

    public static void main(String[] args) throws Exception {
        Map<String,Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG,"numeric");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false,customConverterConfigs)) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "x");

        SingleOutputStreamOperator<OrderBean> process = ds.map((MapFunction<String, JSONObject>) JSON::parseObject)
                .keyBy((KeySelector<JSONObject, Long>) jsonObject -> jsonObject.getJSONObject("after").getLong("id"))
                .process(new KeyedProcessFunction<Long, JSONObject, OrderBean>() {
                    ValueState<OrderBean> orderState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderBean>("od", OrderBean.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<Long, JSONObject, OrderBean>.Context context, Collector<OrderBean> collector) throws Exception {
                        JSONObject after = jsonObject.getJSONObject("after");

                        long modifyTime = after.getLong("modify_time");
                        if(modifyTime < new Date().getTime()) return;

                        // '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单'
                        Integer newStatus = after.getInteger("status");
                        BigDecimal curTotalAmount = after.getBigDecimal("total_amount");
                        BigDecimal curPayAmount = after.getBigDecimal("pay_amount");

                        OrderBean oldOrderBean = orderState.value();
                        if(oldOrderBean == null){
                            oldOrderBean = new OrderBean(context.getCurrentKey(), BigDecimal.ZERO,BigDecimal.ZERO,null);
                            orderState.update(oldOrderBean);
                        }

                        BigDecimal oldTotalAmount = oldOrderBean.getTotalAmount();
                        BigDecimal oldTotalPayAmount = oldOrderBean.getTotalPayAmount();


                        BigDecimal amountDiff = BigDecimal.ZERO;
                        BigDecimal payAmountDiff = BigDecimal.ZERO;
                        // 今日待付总额（原价、应付）
                        if(newStatus == 0) {
                            // 原价总额 差
                            amountDiff = curTotalAmount.subtract(oldTotalAmount);

                            // 应付总额 差
                            payAmountDiff = curPayAmount.subtract(oldTotalPayAmount);

                        }else if(newStatus == 4 || newStatus == 5){
                            // 原价总额 差
                            amountDiff = BigDecimal.ZERO.subtract(oldTotalAmount);

                            // 应付总额 差
                            payAmountDiff = BigDecimal.ZERO.subtract(oldTotalPayAmount);
                        }

                        collector.collect(new OrderBean(context.getCurrentKey() ,amountDiff,payAmountDiff,null));

                        orderState.update(new OrderBean(context.getCurrentKey(), curTotalAmount,curPayAmount,null));

                        // 今日订单总额

                    }
                });

        process.keyBy(s->1).process(new KeyedProcessFunction<Integer, OrderBean, String>() {
            ValueState<BigDecimal> totalState;
            ValueState<BigDecimal> payState;

            @Override
            public void open(Configuration parameters) throws Exception {
                totalState = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("total", BigDecimal.class));
                payState = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("total", BigDecimal.class));


            }

            @Override
            public void processElement(OrderBean orderBean, KeyedProcessFunction<Integer, OrderBean, String>.Context context, Collector<String> collector) throws Exception {
                if(totalState.value() == null) {
                    totalState.update(BigDecimal.ZERO);
                }

                if(payState.value() == null) {
                    payState.update(BigDecimal.ZERO);
                }

                BigDecimal totalAmount = orderBean.getTotalAmount();
                BigDecimal totalPayAmount = orderBean.getTotalPayAmount();

                totalState.update(totalState.value().add(totalAmount));
                payState.update(payState.value().add(totalPayAmount));

                collector.collect(totalState.value() + "," +payState.value());

            }
        }).setParallelism(1).print();

        env.execute();

    }


    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResultBean{
        private Timestamp window_end;
        private BigDecimal amount;
    }

    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderBean{
        private Long id;
        private BigDecimal totalAmount;
        private BigDecimal totalPayAmount;
        private Integer status;
    }

}

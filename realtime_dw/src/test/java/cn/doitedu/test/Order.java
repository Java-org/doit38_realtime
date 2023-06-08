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

        SingleOutputStreamOperator<BigDecimal> process = ds.map((MapFunction<String, JSONObject>) JSON::parseObject)
                .keyBy((KeySelector<JSONObject, Long>) jsonObject -> jsonObject.getJSONObject("after").getLong("id"))
                .process(new KeyedProcessFunction<Long, JSONObject, BigDecimal>() {
                    ValueState<OrderBean> orderState;
                    ValueState<BigDecimal> sumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderBean>("od", OrderBean.class));
                        sumState = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sum", BigDecimal.class));

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<Long, JSONObject, BigDecimal>.Context context, Collector<BigDecimal> collector) throws Exception {
                        JSONObject after = jsonObject.getJSONObject("after");
                        // '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单'
                        Integer status = after.getInteger("status");
                        BigDecimal totalAmount = after.getBigDecimal("total_amount");

                        OrderBean oldOrderBean = orderState.value();

                        if (oldOrderBean == null && status > 0) {
                            collector.collect(totalAmount);
                        }

                        if (oldOrderBean != null && !oldOrderBean.getTotalAmount().equals(totalAmount)) {
                            collector.collect(totalAmount.subtract(oldOrderBean.getTotalAmount()));
                        }
                    }
                });

        process.keyBy(d->1).process(new ProcessFunction<BigDecimal, BigDecimal>() {
            ValueState<BigDecimal> s;
            @Override
            public void open(Configuration parameters) throws Exception {
                s = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("s", BigDecimal.class));
            }

            @Override
            public void processElement(BigDecimal bigDecimal, ProcessFunction<BigDecimal, BigDecimal>.Context context, Collector<BigDecimal> collector) throws Exception {
                BigDecimal old = s.value();
                if(old == null) {
                    old = BigDecimal.valueOf(0);
                    s.update(old);
                }

                s.update(old.add(bigDecimal));
                collector.collect(s.value());
            }
        }).print();


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
        private Integer status;
    }

}

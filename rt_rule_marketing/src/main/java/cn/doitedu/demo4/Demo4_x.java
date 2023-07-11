package cn.doitedu.demo4;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.HashMap;
import java.util.Map;

public class Demo4_x {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-gxx")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");
        SingleOutputStreamOperator<UserEvent> beanSteam = ds.map(s -> JSON.parseObject(s, UserEvent.class));

        // keyBy(user_id)
        beanSteam.keyBy(UserEvent::getUser_id)
                .process(new KeyedProcessFunction<Long, UserEvent, String>() {

                    HashMap<String, RuleCalculator> calculatorPool = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // 构造规则1的运算机
                        Rule1Calculator_x rule1CalculatorX = new Rule1Calculator_x();
                        // 初始化规则1的运算机
                        rule1CalculatorX.init("",getRuntimeContext());


                        // 构造规则2的运算机
                        Rule2Calculator_x rule2CalculatorX = new Rule2Calculator_x();
                        // 初始化规则1的运算机
                        rule2CalculatorX.init("",getRuntimeContext());

                        // 把构造好的运算机，放入运算机池
                        calculatorPool.put("rule_001",rule1CalculatorX);
                        calculatorPool.put("rule_002",rule2CalculatorX);

                    }

                    @Override
                    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, String>.Context ctx, Collector<String> collector) throws Exception {

                        for (Map.Entry<String, RuleCalculator> entry : calculatorPool.entrySet()) {
                            RuleCalculator calculator = entry.getValue();
                            calculator.calculate(userEvent,collector);
                        }

                    }
                })
                .print();

        env.execute();

    }
}

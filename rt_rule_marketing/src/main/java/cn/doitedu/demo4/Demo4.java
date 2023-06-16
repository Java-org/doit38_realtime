package cn.doitedu.demo4;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Collection;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/13
 * @Desc: 学大数据，上多易教育
 * 实时监控app上的所有用户的所有行为
 * 相较 demo3的变化： 规则处理逻辑的模块化抽离; 抽象出规则处理器的统一接口：规则运算机
**/
public class Demo4 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-gxx")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加source，得到源数据流
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // json解析成javaBean
        SingleOutputStreamOperator<UserEvent> beanStream = ds.map(json -> JSON.parseObject(json, UserEvent.class));

        // 规则是否满足的判断核心逻辑
        SingleOutputStreamOperator<String> messages = beanStream.keyBy(UserEvent::getUser_id)
                .process(new KeyedProcessFunction<Long, UserEvent, String>() {

                    HashMap<String,RuleCalculator> calculatorMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // 构造规则1的运算机，并为它初始化
                        RuleCalculator rule1Calculator = new Rule1Calculator();
                        String param1 = "{\"ruleId\":\"rule-001\",\"static_profile\":[{\"tag_name\":\"age\",\"compare_op\":\">\",\"compare_value\":30},{\"tag_name\":\"gender\",\"compare_op\":\"=\",\"compare_value\":[\"male\"]}],\"dynamic_profile\":[{\"event_id\":\"w\",\"event_cnt\":3}],\"fire_event\":{\"event_id\":\"x\",\"properties\":[{\"property_name\":\"p1\",\"compare_op\":\"=\",\"compare_value\":\"v1\"}]}}\n";
                        rule1Calculator.init(param1,getRuntimeContext());

                        calculatorMap.put("rule-001",rule1Calculator);


                        // 构造规则2的运算机，并为它初始化
                        RuleCalculator rule2Calculator = new Rule2Calculator();
                        String param2 = "{\"rule_id\":\"rule-002\",\"event_id\":\"x\"}";
                        rule2Calculator.init(param2,getRuntimeContext());

                        calculatorMap.put("rule-002",rule2Calculator);


                        // 构造规则3的运算机，并为它初始化
                        RuleCalculator rule3Calculator = new Rule2Calculator();
                        String param3 = "{\"rule_id\":\"rule-003\",\"event_id\":\"y\"}";
                        rule3Calculator.init(param3,getRuntimeContext());

                        calculatorMap.put("rule-003",rule3Calculator);
                    }

                    @Override
                    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, String>.Context context, Collector<String> collector) throws Exception {

                        Collection<RuleCalculator> calculators = calculatorMap.values();
                        for (RuleCalculator calculator : calculators) {
                            // 调用 规则运算机 ，处理当前所接收到的 用户行为数据
                            calculator.calculate(userEvent,collector);
                        }

                    }

                });

    }
}

package cn.doitedu.demo5;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.HashMap;

public class Demo5 {

    public static void main(String[] args) {

        // 构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");


        // 构造kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setGroupId("gx001")
                .setTopics("dwd_events")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();


        // 利用kafka source读取数据得到输入流
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");


        // json解析
        SingleOutputStreamOperator<UserEvent> eventStream = stream.map(json -> JSON.parseObject(json, UserEvent.class));


        // keyBy(user_id)
        KeyedStream<UserEvent, Long> keyedEventStream = eventStream.keyBy(event -> event.getUser_id());


        // process
        SingleOutputStreamOperator<String> result = keyedEventStream.process(new ProcessFunction<UserEvent, String>() {

            HashMap<String, RuleCalculator> calculatorPool = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {

                // 构造规则运算机

                // 初始化运算机

                // 放入运算机池

            }

            @Override
            public void processElement(UserEvent userEvent, ProcessFunction<UserEvent, String>.Context ctx, Collector<String> out) throws Exception {

                for (RuleCalculator calculator : calculatorPool.values()) {
                    calculator.calculate(userEvent,out);
                }

            }

        });

        //
        result.print();


    }
}

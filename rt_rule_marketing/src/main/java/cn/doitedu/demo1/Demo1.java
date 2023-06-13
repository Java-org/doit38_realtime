package cn.doitedu.demo1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/13
 * @Desc: 学大数据，上多易教育
 *   实时监控app上的所有用户的所有行为
 *   规则 1： 当某用户发生了 X 行为，立刻推出消息
 *   规则 2： 当某用户发生了 c 行为，且行为属性中符合  properties[p1] = v1
 *
 **/
public class Demo1 {
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
                    JSONObject message;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        message = new JSONObject();
                    }

                    @Override
                    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, String>.Context context, Collector<String> collector) throws Exception {

                        // 规则1
                        if (userEvent.getEvent_id().equals("x")) {

                            message.put("user_id", context.getCurrentKey());
                            message.put("match_time", userEvent.getEvent_time());
                            message.put("rule_id", "rule-001");

                            collector.collect(message.toJSONString());
                        }


                        // 规则2
                        if (userEvent.getEvent_id().equals("c")
                        && userEvent.getProperties().getOrDefault("p1","").equals("v1")
                        ) {
                            message.put("user_id", context.getCurrentKey());
                            message.put("match_time", userEvent.getEvent_time());
                            message.put("rule_id", "rule-002");

                            collector.collect(message.toJSONString());
                        }


                    }
                });

        messages.print();

        env.execute();

    }
}

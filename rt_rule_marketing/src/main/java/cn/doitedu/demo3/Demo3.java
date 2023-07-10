package cn.doitedu.demo3;

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

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/13
 * @Desc: 学大数据，上多易教育
 * 实时监控app上的所有用户的所有行为
 * 相较 demo2的变化： 支持动态实时画像统计
 * 规则 1： 当 画像标签 age>30 and age<40 AND gender=male[静态画像条件]，在规则上线后，如果该用户发生了 3次 w行为后[实时统计画像标签条件] ，在发生 u 行为[触发条件]，立刻推出消息
 * 规则 2： 当 画像标签 active_level=2  AND gender=male[静态画像条件] 在规则上线后，用户依次发生过：(k, b ,c) [实时统计画像标签条件]，当发生 m行为[触发条件]，推送消息
 **/
public class Demo3 {
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
                    Connection connection;
                    Table table;

                    ValueState<Integer> wCntState;
                    ValueState<Integer> rule2State;

                    // 规则2中的动态画像条件参数
                    String[] rule2EventSeq = {"k", "b", "c"};

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        message = new JSONObject();

                        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                        config.set("hbase.zookeeper.quorum", "doitedu:2181");

                        connection = ConnectionFactory.createConnection(config);
                        table = connection.getTable(TableName.valueOf("user_profile"));


                        wCntState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("r1_cnt", Integer.class));

                        rule2State = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("r2_cnt", Integer.class));

                    }

                    @Override
                    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, String>.Context context, Collector<String> collector) throws Exception {

                        long userId = userEvent.getUser_id();

                        // 规则 1 :
                        //  静态画像条件： 画像标签 age>30 and age<40 AND gender=male
                        //  实时动态画像标签条件： 在规则上线后，该用户发生 3次 w 行为
                        //  规则触发条件： 发生x行为

                        // 动态画像条件统计
                        if (userEvent.getEvent_id().equals("w")) {
                            wCntState.update(wCntState.value() == null ? 1 : wCntState.value() + 1);
                        }

                        // 规则被触发后的判断
                        if (userEvent.getEvent_id().equals("x")) {
                            // 先判断动态统计画像条件是否已满足
                            if (wCntState.value() != null && wCntState.value() >= 3) {

                                // 如果动态画像条件以满足，进而判断该受众是否满足规则中的静态画像条件
                                Get get = new Get(Bytes.toBytes(userId));
                                get.addColumn("f".getBytes(), "age".getBytes());
                                get.addColumn("f".getBytes(), "gender".getBytes());

                                Result result = table.get(get);
                                byte[] ageBytes = result.getValue("f".getBytes(), "age".getBytes());
                                int age = Bytes.toInt(ageBytes);

                                byte[] genderBytes = result.getValue("f".getBytes(), "gender".getBytes());
                                String gender = Bytes.toString(genderBytes);

                                if (age == 30 && "male".equals(gender)) {
                                    // 如果静态画像也满足，则输出规则命中消息
                                    message.put("user_id", userId);
                                    message.put("match_time", userEvent.getEvent_time());
                                    message.put("rule_id", "rule-001");

                                    collector.collect(message.toJSONString());
                                }
                            }


                        }


                        /**
                         * 规则 2 :
                         * 静态画像条件： active_level=2  AND gender=male
                         * 动态画像条件： 规则上线后，依次发生过：[k, b ,c]行为序列
                         * 触发条件： 发生 m 行为(  eventId=c,properties[p1]=v1 )
                         */

                        // 动态画像统计逻辑
                        Integer seqNo = rule2State.value();
                        if (seqNo == null) seqNo = 0;
                        if (seqNo < 3) {
                            if (userEvent.getEvent_id().equals(rule2EventSeq[seqNo])) {
                                rule2State.update(seqNo + 1);
                            }
                        }

                        // 规则触发判断逻辑
                        if (
                                userEvent.getEvent_id().equals("c")
                                        && userEvent.getProperties().getOrDefault("p1", "").equals("v1")
                        ) {
                            // 进而判断动态画像条件是否满足
                            if (rule2State.value() != null && rule2State.value() >= 3) {

                                // 进而判断用户的静态画像条件是否满足
                                Get get = new Get(Bytes.toBytes(userId));
                                get.addColumn("f".getBytes(), "active_level".getBytes());
                                get.addColumn("f".getBytes(), "gender".getBytes());

                                Result result = table.get(get);
                                byte[] activeBytes = result.getValue("f".getBytes(), "active_level".getBytes());
                                int activeLevel = Bytes.toInt(activeBytes);

                                byte[] genderBytes = result.getValue("f".getBytes(), "gender".getBytes());
                                String gender = Bytes.toString(genderBytes);

                                if (activeLevel == 3 && "male".equals(gender)) {
                                    // 如果静态画像条件也满足，则输出规则命中消息
                                    message.put("user_id", context.getCurrentKey());
                                    message.put("match_time", userEvent.getEvent_time());
                                    message.put("rule_id", "rule-002");

                                    collector.collect(message.toJSONString());
                                }
                            }
                        }
                    }
                });

        messages.print();
        env.execute();

    }
}

package cn.doitedu.demo2;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/13
 * @Desc: 学大数据，上多易教育
 * 实时监控app上的所有用户的所有行为
 * 相较demo1的变化： 规则中，对目标受众，添加了画像约束
 * 规则 1： 当 画像标签 age>=30 and age<=40 AND gender=male  用户发生了 x 行为，立刻推出消息
 * 规则 2： 当 画像标签 active_level=3  AND gender=female 用户发生了 c 行为，且行为属性中符合  properties[p1] = v1
 **/
public class Demo2_1 {
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
        SingleOutputStreamOperator<String> messages =
                beanStream
                        .keyBy(UserEvent::getUser_id)
                        .process(new KeyedProcessFunction<Long, UserEvent, String>() {
                            JSONObject message;
                            HashSet<Long> rule1PreSelectedUsers;
                            HashSet<Long> rule2PreSelectedUsers;
                            Roaring64Bitmap rule1PreSelectedBitmap;
                            Roaring64Bitmap rule2PreSelectedBitmap;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                message = new JSONObject();

                                // 规则 1 的静态画像条件预圈选人群
                                //rule1PreSelectedUsers = new HashSet<>(Arrays.asList(1L, 3L, 5L));
                                rule1PreSelectedBitmap = Roaring64Bitmap.bitmapOf(1L, 3L, 5L);


                                // 规则 2 的静态画像条件预圈选人群
                                //rule2PreSelectedUsers = new HashSet<>(Arrays.asList(2L, 3L, 6L));
                                rule2PreSelectedBitmap = Roaring64Bitmap.bitmapOf(2L, 3L, 6L);


                            }

                            @Override
                            public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, String>.Context context, Collector<String> collector) throws Exception {

                                long userId = userEvent.getUser_id();

                                // 规则1:  画像标签 age=30 AND gender=male  用户发生了 x 行为，立刻推出消息
                                if (userEvent.getEvent_id().equals("x") && rule1PreSelectedBitmap.contains(userId) ) {
                                    message.put("hit_rule_id","rule_1");
                                    message.put("hit_user_id",userId);
                                    message.put("hit_timestamp",userEvent.getEvent_time());

                                    collector.collect(message.toJSONString());
                                }


                                // 规则2 :画像标签 active_level=3  AND gender=female 用户发生了 c 行为，且行为属性中符合  properties[p1] = v1
                                if (    userEvent.getEvent_id().equals("c")
                                        && userEvent.getProperties().getOrDefault("p1", "").equals("v1")
                                        && rule2PreSelectedBitmap.contains(userId)
                                ) {
                                    message.put("hit_rule_id","rule_1");
                                    message.put("hit_user_id",userId);
                                    message.put("hit_timestamp",userEvent.getEvent_time());

                                    collector.collect(message.toJSONString());
                                }
                            }
                        });

        messages.print();

        env.execute();

    }
}

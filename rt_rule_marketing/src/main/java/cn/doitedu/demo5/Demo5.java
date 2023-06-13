package cn.doitedu.demo5;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
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
 * 相较 demo4的变化： 规则的参数，是从外部传入的
 **/
public class Demo5 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

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
        SingleOutputStreamOperator<UserEvent> userEventBeanStream = ds.map(json -> JSON.parseObject(json, UserEvent.class));


        /**
         * 用cdc去监听规则的元数据库
         */
        tenv.executeSql(
                "CREATE TABLE rule_meta_mysql (    " +
                        "      rule_id STRING," +
                        "      rule_model_id STRING," +
                        "      rule_param_json STRING" +
                        "     PRIMARY KEY (rule_id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'doit38',         " +
                        "     'table-name' = 'rule_meta'          " +
                        ")"
        );
        DataStream<Row> ruleMetaStream = tenv.toChangelogStream(tenv.from("rule_meta_mysql"));
        SingleOutputStreamOperator<RuleMetaBean> ruleMetaBeanStream = ruleMetaStream.map(new MapFunction<Row, RuleMetaBean>() {
            @Override
            public RuleMetaBean map(Row row) throws Exception {
                String ruleId = row.getFieldAs("rule_id");
                String ruleModelId = row.getFieldAs("rule_model_id");
                String ruleParamJson = row.getFieldAs("rule_param_json");
                RowKind kind = row.getKind();
                String op = kind.shortString();

                return new RuleMetaBean(op, ruleId, ruleModelId, ruleParamJson);
            }
        });

        // 广播规则定义数据
        MapStateDescriptor<String, RuleCalculator> desc = new MapStateDescriptor<>("calculator-map", String.class, RuleCalculator.class);
        BroadcastStream<RuleMetaBean> broadcast = ruleMetaBeanStream.broadcast(desc);


        // 规则是否满足的判断核心逻辑
        SingleOutputStreamOperator<String> messages
                = userEventBeanStream
                .keyBy(UserEvent::getUser_id)
                .connect(broadcast)  // 用户行为数据流  连接  规则元数据广播流
                .process(new KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>() {
                    @Override
                    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

                    }

                    @Override
                    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.Context context, Collector<String> collector) throws Exception {




                    }
                });

    }
}

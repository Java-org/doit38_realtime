package cn.doitedu.demo5;

import cn.doitedu.demo5.beans.UserEvent;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.util.HashMap;

public class Demo5 {

    public static void main(String[] args) throws Exception {

        // 构建flink环境
        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.network.min","128 M");
        conf.setString("taskmanager.memory.network.max","128 M");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");


        // 构造kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setGroupId("gx001")
                .setTopics("dwd_events")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
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

                // 获取一个查询离线画像条件的hbase连接
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "doitedu:2181");
                Connection connection = ConnectionFactory.createConnection(configuration);
                Table tablex = connection.getTable(TableName.valueOf("user_profile"));

                org.apache.hadoop.conf.Configuration configuration2 = HBaseConfiguration.create();
                configuration2.set("hbase.zookeeper.quorum", "doitedu:2181");
                Connection connection2 = ConnectionFactory.createConnection(configuration2);
                Table tabley = connection2.getTable(TableName.valueOf("user_profile"));


                // 构造模型1的规则1的运算机
                RuleCalculator rule_1_1 = new RuleModel_1_Calculator();
                String rule_1_1ParamJson = "{\"rule_id\":\"rule_1_1\",\"offline_profile\":[{\t\t\"tag_name\":\"age\",\t\t\"compare_type\":\"between\",\t\t\"tag_value\":[20,30]\t},\t{\t\t\"tag_name\":\"gender\",\t\t\"compare_type\":\"=\",\t\t\"tag_value\":[\"male\"]\t}],\"online_profile\":{\t\"event_id\":\"add_cart\",\t\"event_count\":3},\"fire_event\":{\t\"event_id\":\"x\",\t\"pro_name\":\"p1\",\t\"pro_value\":\"v1\"}}";
                rule_1_1.init(rule_1_1ParamJson,getRuntimeContext());
                calculatorPool.put("rule_1_1",rule_1_1);

                // 构造模型1的规则2的运算机
                /*RuleCalculator rule_1_2 = new RuleModel_1_Calculator();
                String rule_1_2ParamJson = "{\"rule_id\":\"rule_1_2\",\"offline_profile\":[{\t\t\"tag_name\":\"age\",\t\t\"compare_type\":\">\",\t\t\"tag_value\":[25]\t},\t{\t\t\"tag_name\":\"gender\",\t\t\"compare_type\":\"=\",\t\t\"tag_value\":[\"female\"]\t}],\"online_profile\":{\t\"event_id\":\"item_share\",\t\"event_count\":2},\"fire_event\":{\t\"event_id\":\"add_cart\",\t\"pro_name\":\"item_id\",\t\"pro_value\":\"p0008\"}}";
                rule_1_2.init(rule_1_2ParamJson,getRuntimeContext());
                calculatorPool.put("rule_1_2",rule_1_2);*/

                /*RuleModel_2_Calculator rule_2_1 = new RuleModel_2_Calculator();
                String rule_2_1ParamJson = "{\"rule_id\":\"rule_2_1\",\"offline_profile\":[{\"tag_name\":\"active_level\",\"compare_type\":\"=\",\"tag_value\":[2]},{\"tag_name\":\"gender\",\"compare_type\":\"=\",\"tag_value\":[\"male\"]}],\"online_profile\":{\"event_seq\":[\"k\",\"b\",\"c\"],\"seq_count\":3},\"fire_event\":{\"event_id\":\"share\",\"pro_name\":\"share_method\",\"pro_value\":\"qq\"}}";
                rule_2_1.init(rule_2_1ParamJson,getRuntimeContext());
                calculatorPool.put("rule_2_1",rule_2_1);
*/
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


        env.execute();

    }
}

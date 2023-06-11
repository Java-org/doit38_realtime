package cn.doitedu.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

public class TestZone {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource1 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("test") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("test.test_zone") // set captured table
                .username("root")
                .password("root")
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> s1 = env.fromSource(mySqlSource1, WatermarkStrategy.noWatermarks(), "cdc");
        s1.print();

        env.execute();
    }
}

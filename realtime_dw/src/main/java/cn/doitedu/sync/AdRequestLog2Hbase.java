package cn.doitedu.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/8
 * @Desc: 学大数据，上多易教育
 * <p>
 * 广告请求特征数据 同步到  hbase
 **/
public class AdRequestLog2Hbase {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/xx/yy/ckpt/");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建kafka连接器表，映射kafka中存放请求特征日志数据的topic
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("ad_req_log")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("ad_feature_group")
                .build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ad_req");
        SingleOutputStreamOperator<Tuple2<String, String>> idStream = stream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String json) throws Exception {

                JSONObject jsonObject = JSON.parseObject(json);
                String adTrackingId = jsonObject.getString("ad_tracking_id");
                return Tuple2.of(adTrackingId, json);
            }
        });

        // 流转表
        tenv.createTemporaryView("id_log",idStream);


        // 2. 创建 hbase 连接器表，映射 hbase中存放请求特征日志数据的table
        tenv.executeSql(
                " create table ad_req_hbase(                        " +
                        "    ad_tracking_id STRING,                  " +
                        "    f  ROW<log STRING>                      " +
                        " ) WITH(                                    " +
                        "     'connector' = 'hbase-2.2',             " +
                        "     'table-name' = 'ad_request_log',       " +
                        "     'zookeeper.quorum' = 'doitedu:2181'    " +
                        " )                                          "
        );


        // 3. insert  select
        tenv.executeSql("insert into ad_req_hbase select f0,row(f1) from id_log");

    }
}

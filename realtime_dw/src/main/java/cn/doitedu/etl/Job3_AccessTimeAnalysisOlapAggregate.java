package cn.doitedu.etl;

import cn.doitedu.beans.TimelongBean;
import cn.doitedu.functions.TimeLongProcessFunction;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/6
 * @Desc: 学大数据，上多易教育
 * <p>
 * 访问时长olap分析的支撑etl聚合任务
 * 聚合粒度： 每个人每次打开一个页面的  ： 起始时间，结束时间
 **/
public class Job3_AccessTimeAnalysisOlapAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建映射表，映射kafka明细层的行为日志
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-gxx")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加source，得到源数据流
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // 将源数据流中的json格式数据，转成javaBean数据
        SingleOutputStreamOperator<TimelongBean> beanStream = ds.map(json -> {

            TimelongBean timelongBean = JSON.parseObject(json, TimelongBean.class);
            timelongBean.setPage_url(timelongBean.getProperties().get("url"));
            timelongBean.setProperties(null);

            return timelongBean;
        });


        // 核心逻辑
        SingleOutputStreamOperator<TimelongBean> resultStream = beanStream
                .keyBy(bean -> bean.getSession_id())
                .process(new TimeLongProcessFunction());


        /*
         +---------+--------------------------------+--------------------+----------------------+
         | user_id |                       page_url |    page_start_time |        page_end_time |
         +---------+--------------------------------+--------------------+----------------------+
         |       3 |                          index |      1675407922100 |        1675407922100 |
         |       3 |                          index |      1675407922100 |        1675407922000 |
         |       3 |                          index |      1675407922100 |        1675407923000 |
         |       3 |                          index |      1675407922100 |        1675407924000 |
         |       3 |                         page08 |      1675407924000 |        1675407924000 |
         |       3 |                         page08 |      1675407924000 |        1675407925000 |
         |       3 |                         page08 |      1675407924000 |        1675407926000 |
         |       3 |                         page08 |      1675407924000 |        1675407927000 |
         |       3 |                         page08 |      1675407935000 |        1675407935000 |
         |       3 |                         page08 |      1675407935000 |        1675407937000 |
         |       3 |                         page08 |      1675407935000 |        1675407938000 |
         |       3 |                         page08 |      1675407935000 |        1675407940000 |
         */
        // 将整理好结构的数据流，转成表，来做后续的聚合（聚合后写入doris更合理）
        Schema schema = Schema.newBuilder()
                .column("user_id", DataTypes.BIGINT())
                .column("device_type", DataTypes.STRING())
                .column("release_channel", DataTypes.STRING())
                .column("session_id", DataTypes.STRING())
                .column("page_type", DataTypes.STRING())
                .column("page_url", DataTypes.STRING())
                .column("page_start_time", DataTypes.BIGINT())
                .column("page_end_time", DataTypes.BIGINT())
                .columnByExpression("row_time","to_timestamp_ltz(page_start_time,3)")
                .watermark("row_time","row_time - interval '0' second")
                .build();


        // 创建doris目标表的连接器映射表
        tenv.executeSql(
                "CREATE TABLE sink_doris(                             "+
                        "    dt       DATE,                              "+
                        "    user_id  BIGINT,                            "+
                        "    device_type  VARCHAR(20),                   "+
                        "    release_channel VARCHAR(20),                "+
                        "    session_id  VARCHAR(20),                    "+
                        "    page_type  VARCHAR(20),                     "+
                        "    page_url  VARCHAR(20),                      "+
                        "    page_start_time   BIGINT ,                  "+
                        "    page_end_time  BIGINT                       "+
                ") WITH (                                                "+
                        "   'connector' = 'doris',                       "+
                        "   'fenodes' = 'doitedu:8030',                  "+
                        "   'table.identifier' = 'dws.acc_timelong_page', "+
                        "   'username' = 'root',                         "+
                        "   'password' = 'root',                         "+
                        "   'sink.label-prefix' = 'doris_label66'         "+
                        ")                                               "
        );




        tenv.createTemporaryView("tmp",resultStream, schema);
        tenv.executeSql(

                " INSERT INTO sink_doris  SELECT                                                                           "+
                        "   to_date(date_format(to_timestamp_ltz(page_start_time,3),'yyyy-MM-dd')) as dt,  "+
                        "   user_id,                                                                       "+
                        "   device_type,                                                                   "+
                        "   release_channel,                                                               "+
                        "   session_id,                                                                    "+
                        "   page_type,                                                                     "+
                        "   page_url,                                                                      "+
                        "   page_start_time,                                                               "+
                        "   max(page_end_time) as page_end_time                                            "+
                        " FROM TABLE(                                                                      "+
                        "   TUMBLE(TABLE tmp,DESCRIPTOR(row_time),INTERVAL '5' MINUTE)                     "+
                        " )                                                                                "+
                        " GROUP BY                                                                         "+
                        "   window_start,                                                                  "+
                        "   window_end,                                                                    "+
                        "   user_id,                                                                       "+
                        "   device_type,                                                                   "+
                        "   release_channel,                                                               "+
                        "   session_id,                                                                    "+
                        "   page_type,                                                                     "+
                        "   page_url,                                                                      "+
                        "   page_start_time                                                                "
        );


        env.execute();
    }
}

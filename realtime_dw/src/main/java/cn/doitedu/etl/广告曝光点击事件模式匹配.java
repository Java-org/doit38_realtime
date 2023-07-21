package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class 广告曝光点击事件模式匹配 {
    public static void main(String[] args) {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);



        // 建表，映射行为日志明细数据
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                 "
                        + "     user_id           BIGINT,                     "
                        + "     event_id          string,                     "
                        + "     event_time        bigint,                     "
                        + "     properties        map<string,string>,         "
                        + "     proc_time AS proctime()  ,                    "
                        + "     row_time AS to_timestamp_ltz(event_time,3),   "
                        + "     watermark for row_time as row_time - interval '0' second   "
                        + " ) WITH (                                          "
                        + "  'connector' = 'kafka',                           "
                        + "  'topic' = 'dwd_events',                          "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092', "
                        + "  'properties.group.id' = 'testGroup',             "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'value.format'='json',                           "
                        + "  'value.json.fail-on-missing-field'='false',      "
                        + "  'value.fields-include' = 'EXCEPT_KEY')           "
        );

        tenv.executeSql("with events AS (\n" +
                "    SELECT\n" +
                "      user_id,\n" +
                "      event_id,\n" +
                "      event_time,\n" +
                "      properties['creative_id'] as creative_id,\n" +
                "      properties['ad_tracking_id'] as ad_tracking_id,\n" +
                "\t  row_time\n" +
                "    FROM dwd_kafka\n" +
                "    WHERE event_id in ('ad_show','ad_click') \n" +
                ")\n" +
                "\n" +
                "\n" +
                "SELECT\n" +
                "   a_user_id as user_id,\n" +
                "   a_event_id as event_id,\n" +
                "   a_creative_id as creative_id,\n" +
                "   a_ad_tracking_id as ad_tracking_id,\n" +
                "   a_event_time as show_time,\n" +
                "   b_event_time as click_time\n" +
                "\n" +
                "FROM events  \n" +
                "   MATCH_RECOGNIZE(\n" +
                "   \n" +
                "    PARTITION BY ad_tracking_id\n" +
                "\tORDER BY row_time\n" +
                "\tMEASURES\n" +
                "\t  A.user_id  as a_user_id,\n" +
                "\t  A.event_id as a_event_id,\n" +
                "\t  A.event_time as a_event_time,\n" +
                "\t  A.creative_id as a_creative_id,\n" +
                "\t  A.ad_tracking_id as a_ad_tracking_id,\n" +
                "\t  B.event_time as b_event_time\n" +
                "\tONE ROW PER MATCH\n" +
                "\tAFTER MATCH SKIP PAST LAST ROW\n" +
                "\tPATTERN(A B) WITHIN INTERVAL '5' MINUTE\n" +
                "\tDEFINE\n" +
                "\t   A AS A.event_id = 'ad_show',\n" +
                "\t   B AS B.event_id = 'ad_click')").print();


    }
}

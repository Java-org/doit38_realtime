package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/8
 * @Desc: 学大数据，上多易教育
 * <p>
 * 广告点击率预估  ，特征数据etl任务
 **/
public class Job7_AdClickPredictFeatureEtl {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1. 读行为明细
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                 "
                        + "     user_id           BIGINT,                     "
                        + "     username          string,                     "
                        + "     session_id        string,                     "
                        + "     event_id          string,                     "
                        + "     event_time        bigint,                     "
                        + "     lat               double,                     "
                        + "     lng               double,                     "
                        + "     release_channel   string,                     "
                        + "     device_type       string,                     "
                        + "     properties        map<string,string>,         "
                        + "     register_phone    STRING,                     "
                        + "     user_status       INT,                        "
                        + "     register_time     TIMESTAMP(3),               "
                        + "     register_gender   INT,                        "
                        + "     register_birthday DATE,                       "
                        + "     register_province STRING,                     "
                        + "     register_city        STRING,                  "
                        + "     register_job         STRING,                  "
                        + "     register_source_type INT,                     "
                        + "     gps_province STRING,                          "
                        + "     gps_city     STRING,                          "
                        + "     gps_region   STRING,                          "
                        + "     page_type    STRING,                          "
                        + "     page_service STRING,                          "
                        + "     proc_time AS proctime(),                      "
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

        // 2. 过滤广告事件
        tenv.executeSql(
                " CREATE TEMPORARY VIEW events AS SELECT            " +
                        "   user_id,                                        " +
                        "   event_time,                                     " +
                        "   event_id,                                       " +
                        "   properties['ad_tracking_id'] as ad_tracking_id, " +
                        "   properties['creative_id'] as creative_id,       " +
                        "   row_time                                        " +
                        " FROM dwd_kafka                                    " +
                        " WHERE event_id in ('ad_show','ad_click')          "
        );

        // 3. 用 CEP匹配有点击的曝光 - 正例
        tenv.executeSql(
                " CREATE TEMPORARY VIEW  show_click AS      "+
                        "SELECT                               "+
                        "    a_user_id as user_id,            "+
                        "    ad_show_time,                    "+
                        "    a_creative_id as creative_id,    "+
                        "    ad_click_time,                   "+
                        "    tracking_id,                     "+
                        "    proctime() as proc_time          "+
                        " FROM events                         "+
                        " MATCH_RECOGNIZE(                    "+
                        "   PARTITION BY ad_tracking_id       "+
                        "   ORDER BY row_time                 "+
                        "   MEASURES                          "+
                        "   A.user_id as a_user_id,           "+
                        " 	A.event_time as ad_show_time,     "+
                        " 	A.creative_id as a_creative_id,   "+
                        " 	A.ad_tracking_id as tracking_id,  "+
                        " 	B.event_time as ad_click_time     "+
                        "   ONE ROW PER MATCH                 "+
                        "   AFTER MATCH SKIP TO NEXT ROW      "+
                        "   PATTERN(A B) WITHIN INTERVAL '15' MINUTE  "+
                        "   DEFINE                            "+
                        "      A AS A.event_id='ad_show',     "+
                        " 	   B AS B.event_id='ad_click'     "+
                        " )                                   "
        );


        // 4.用 lookup查询，来关联 曝光点击流  和 请求特征流
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

        tenv.executeSql(
                " CREATE TEMPORARY VIEW res AS                                           "+
                        " SELECT                                                             "+
                        "   sc.user_id,                                                      "+
                        "   sc.creative_id,                                                  "+
                        "   sc.ad_show_time,                                                 "+
                        "   sc.ad_click_time,                                                "+
                        "   req.f.log as feature_log                                         "+
                        " FROM show_click sc                                                 "+
                        " LEFT JOIN ad_req_hbase FOR SYSTEM_TIME AS OF sc.proc_time AS req   "+
                        " ON sc.tracking_id = req.ad_tracking_id                             "

        );

        // 5.将计算结果输出到 kafka
        tenv.executeSql(
                "  CREATE TABLE sink_kafka(                                "
                       +"   user_id   bigint                                  "
                       +"   ,creative_id string                               "
                       +"   ,ad_show_time  bigint                             "
                       +"   ,ad_click_time bigint                             "
                       +"   ,feature_log   string                             "
                        + " ) WITH (                                          "
                        + "  'connector' = 'kafka',                           "
                        + "  'topic' = 'ad_realtime_feature_positive',        "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092', "
                        + "  'properties.group.id' = 'testGroup',             "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'value.format'='json',                           "
                        + "  'value.json.fail-on-missing-field'='false',      "
                        + "  'value.fields-include' = 'EXCEPT_KEY')           "
        );

        tenv.executeSql("insert into sink_kafka select * from res");


    }
}

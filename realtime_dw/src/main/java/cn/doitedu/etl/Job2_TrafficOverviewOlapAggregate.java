package cn.doitedu.etl;

import cn.doitedu.udfs.TimeStampTruncate2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量概况多维olap分析用的聚合任务
 *  逻辑： 从kafka明细层读取行为日志，按照流量olap分析主题中要求的最细粒度聚合pv值，写入doris
 */
public class Job2_TrafficOverviewOlapAggregate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建映射表，映射kafka明细层的行为日志
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                "
                        +"     user_id           BIGINT,                     "
                        +"     username          string,                     "
                        +"     session_id        string,                     "
                        +"     event_id          string,                     "
                        +"     event_time        bigint,                     "
                        +"     lat               double,                     "
                        +"     lng               double,                     "
                        +"     release_channel   string,                     "
                        +"     device_type       string,                     "
                        +"     properties        map<string,string>,         "
                        +"     register_phone    STRING,                     "
                        +"     user_status       INT,                        "
                        +"     register_time     TIMESTAMP(3),               "
                        +"     register_gender   INT,                        "
                        +"     register_birthday DATE,                       "
                        +"     register_province STRING,                     "
                        +"     register_city        STRING,                  "
                        +"     register_job         STRING,                  "
                        +"     register_source_type INT,                     "
                        +"     gps_province STRING,                          "
                        +"     gps_city     STRING,                          "
                        +"     gps_region   STRING,                          "
                        +"     page_type    STRING,                          "
                        +"     page_service STRING,                          "
                        +"     proc_time AS proctime(),                      "
                        +"     row_time AS to_timestamp_ltz(event_time,3),   "
                        +"     watermark for row_time as row_time - interval '0' second   "
                        +" ) WITH (                                          "
                        +"  'connector' = 'kafka',                           "
                        +"  'topic' = 'dwd_events',                         "
                        +"  'properties.bootstrap.servers' = 'doitedu:9092', "
                        +"  'properties.group.id' = 'testGroup',             "
                        +"  'scan.startup.mode' = 'latest-offset',           "
                        +"  'value.format'='json',                           "
                        +"  'value.json.fail-on-missing-field'='false',      "
                        +"  'value.fields-include' = 'EXCEPT_KEY')           "
        );


        // 2. 创建映射表，映射doris中的聚合表
        tenv.executeSql(
                "CREATE TABLE sink_doris(                           "+
                        "    dt       DATE,                                 "+
                        "    time_60m STRING,                               "+
                        "    time_30m STRING,                               "+
                        "    time_10m STRING,                               "+
                        "    time_m   STRING,                               "+
                        "    user_id  BIGINT,                               "+
                        "    is_newuser INT,                                "+
                        "    session_id STRING,                             "+
                        "    release_channel STRING,                        "+
                        "    device_type STRING,                            "+
                        "    gps_province STRING,                           "+
                        "    gps_city STRING,                               "+
                        "    gps_region STRING,                             "+
                        "    page_type STRING,                              "+
                        "    page_service STRING,                           "+
                        "    page_url STRING,                               "+
                        "    pv_amt BIGINT                                  "+
                        ") WITH (                                           "+
                        "   'connector' = 'doris',                          "+
                        "   'fenodes' = 'doitedu:8030',                     "+
                        "   'table.identifier' = 'dws.tfc_overview_u_m',    "+
                        "   'username' = 'root',                            "+
                        "   'password' = 'root',                            "+
                        "   'sink.label-prefix' = 'doris_label3'            "+
                        ")                                                  "
        );

        // 3. 计算逻辑sql
        tenv.createTemporaryFunction("time_trunc", TimeStampTruncate2.class);
        tenv.executeSql(
                " insert into sink_doris                                                                                         "+
                        " with tmp as (                                                                                                  "+
                        " select                                                                                                         "+
                        "   to_date(date_format(to_timestamp_ltz(event_time,3),'yyyy-MM-dd')) as dt                                      "+
                        "   ,time_trunc(event_time,60) as time_60m                                                                       "+
                        "   ,time_trunc(event_time,30) as time_30m                                                                       "+
                        "   ,time_trunc(event_time,10) as time_10m                                                                       "+
                        "   ,time_trunc(event_time,1)  as time_01m                                                                       "+
                        "   ,user_id                                                                                                     "+
                        "   ,if(date_format(register_time,'yyyy-MM-dd')< DATE_FORMAT(row_time,'yyyy-MM-dd'),0,1) as is_newuser           "+
                        "   ,session_id                                                                                                  "+
                        "   ,release_channel                                                                                             "+
                        "   ,device_type                                                                                                 "+
                        "   ,gps_province                                                                                                "+
                        "   ,gps_city                                                                                                    "+
                        "   ,gps_region                                                                                                  "+
                        "   ,page_type                                                                                                   "+
                        "   ,page_service                                                                                                "+
                        "   ,regexp_extract(properties['url'],'^(.*\\.html).*?') as page_url                                             "+
                        "   ,row_time                                                                                                    "+
                        " from dwd_kafka                                                                                                 "+
                        " where event_id='page_load'                                                                                     "+
                        " )                                                                                                              "+
                        "                                                                                                                "+
                        " SELECT                                                                                                         "+
                        "   dt,time_60m,time_30m,time_10m,time_01m,                                                                      "+
                        "   user_id,is_newuser                                                                                           "+
                        "   ,session_id                                                                                                  "+
                        "   ,release_channel                                                                                             "+
                        "   ,device_type                                                                                                 "+
                        "   ,gps_province                                                                                                "+
                        "   ,gps_city                                                                                                    "+
                        "   ,gps_region                                                                                                  "+
                        "   ,page_type                                                                                                   "+
                        "   ,page_service                                                                                                "+
                        "   ,page_url                                                                                                    "+
                        "   ,count(1) as pv_amt                                                                                          "+
                        "                                                                                                                "+
                        " FROM TABLE(                                                                                                    "+
                        "    TUMBLE(TABLE tmp, DESCRIPTOR(row_time),INTERVAL '5' MINUTE)                                                 "+
                        " )                                                                                                              "+
                        " GROUP BY                                                                                                       "+
                        "   window_start,                                                                                                "+
                        "   window_end,                                                                                                  "+
                        "   dt,time_60m,time_30m,time_10m,time_01m,                                                                      "+
                        "   user_id,is_newuser                                                                                           "+
                        "   ,session_id                                                                                                  "+
                        "   ,release_channel                                                                                             "+
                        "   ,device_type                                                                                                 "+
                        "   ,gps_province                                                                                                "+
                        "   ,gps_city                                                                                                    "+
                        "   ,gps_region                                                                                                  "+
                        "   ,page_type                                                                                                   "+
                        "   ,page_service                                                                                                "+
                        "   ,page_url                                                                                                    "
        );

    }
}

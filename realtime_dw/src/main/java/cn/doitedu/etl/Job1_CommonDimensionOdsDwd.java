package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 用户行为日志数据，公共维度退维
 *   本job的主要任务：
 *      1. 从kafka的 ods_events 中读取用户行为数据
 *      2. 对读取到的行为数据去关联hbase中的各个维表（用户注册信息，页面信息，地域信息）
 *      3. 将关联好的结果写入kafka的 dwd_events  和  doris的dwd层表
 */
public class Job1_CommonDimensionOdsDwd {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 建表映射  kafka中行为日志原始数据topic
        tenv.executeSql(
                " CREATE TABLE events_source (                               "
                        // 物理字段
                        + "     username     string,                            "
                        + "     session_id   string,                            "
                        + "     eventId      string,                            "
                        + "     actionTime   bigint,                            "
                        + "     lat          double,                            "
                        + "     lng          double,                            "
                        + "     release_channel   string,                       "
                        + "     device_type       string,                       "
                        + "     properties   map<string,string>,                "
                        // 表达式字段
                        + "     proc_time   AS  proctime(),                    "  // 处理时间语义的时间字段
                        + "     event_time  AS  to_timestamp_ltz(actionTime,3),"  // 提取数据中的actionTime作为事件时间语义的时间
                        // watermark定义语法
                        + "     watermark for event_time as event_time - interval '0' second "  // 声明watermark 为  事件时间-0s乱序
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'ods_events',                            "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'goo1',                    "
                        + "  'scan.startup.mode' = 'latest-offset',             "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )                                                   ");



        // 2. 建表映射  kafka中行为日志dwd 目标topic
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                "
                        +"     user_id           BIGINT,                     "
                        +"     username          string,                     "
                        +"     session_id        string,                     "
                        +"     event_Id          string,                     "
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
                        +"     page_service STRING                           "
                        +" ) WITH (                                          "
                        +"  'connector' = 'kafka',                           "
                        +"  'topic' = 'dwd_events',                         "
                        +"  'properties.bootstrap.servers' = 'doitedu:9092', "
                        +"  'properties.group.id' = 'testGroup',             "
                        +"  'scan.startup.mode' = 'earliest-offset',         "
                        +"  'value.format'='json',                           "
                        +"  'value.json.fail-on-missing-field'='false',      "
                        +"  'value.fields-include' = 'EXCEPT_KEY')           "
        );



        // 3. 建表映射  doris中行为日志dwd目标表
        tenv.executeSql(
                " CREATE TABLE dwd_doris  (         "
                        + "     gps_province         VARCHAR(16),   "
                        + "     gps_city             VARCHAR(16),   "
                        + "     gps_region           VARCHAR(16),   "
                        + "     dt                   DATE,          "
                        + "     user_id              BIGINT,           "
                        + "     username             VARCHAR(20),   "
                        + "     session_id           VARCHAR(20),   "
                        + "     event_id             VARCHAR(10),   "
                        + "     event_time           bigint,        "
                        + "     lat                  DOUBLE,        "
                        + "     lng                  DOUBLE,        "
                        + "     release_channel      VARCHAR(20),   "
                        + "     device_type          VARCHAR(20),   "
                        + "     properties           VARCHAR(40),   "  // doris中不支持Map类型
                        + "     register_phone       VARCHAR(20),   "
                        + "     user_status          INT,           "
                        + "     register_time        TIMESTAMP(3),  "
                        + "     register_gender      INT,           "
                        + "     register_birthday    DATE,          "
                        + "     register_province    VARCHAR(20),   "
                        + "     register_city        VARCHAR(20),   "
                        + "     register_job         VARCHAR(20),   "
                        + "     register_source_type INT        ,   "
                        + "     page_type            VARCHAR(20),   "
                        + "     page_service         VARCHAR(20)    "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'doitedu:8030',         "
                        + "    'table.identifier' = 'dwd.user_events_detail',  "
                        + "    'username' = 'root',                "
                        + "    'password' = 'root',                "
                        + "    'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'"   // 在测试时反复运行防止label已存在
                        + " )                                         "
        );



        // 4. 建表映射  hbase中的维表：注册信息表
        tenv.executeSql(
                " create table user_hbase(                       "+
                        "    username STRING,                        "+
                        "    f1 ROW<                                 "+
                        "       id BIGINT,                           "+
                        " 	    phone STRING,                          "+
                        " 	    status INT,                            "+
                        " 	    create_time TIMESTAMP(3),              "+
                        "       gender INT,                          "+
                        " 	    birthday DATE,                         "+
                        " 	    province STRING,                       "+
                        " 	    city STRING,                           "+
                        " 	    job STRING,                            "+
                        " 	    source_type INT>                       "+
                        " ) WITH(                                    "+
                        "     'connector' = 'hbase-2.2',             "+
                        "     'table-name' = 'dim_user_info',        "+
                        "     'zookeeper.quorum' = 'doitedu:2181'    "+
                        " )                                          "

        );

        // 5. 建表映射  hbase中的维表：页面信息表
        tenv.executeSql(
                " create table page_hbase(                        "+
                        "    url STRING,                             "+
                        "    f  ROW<                                 "+
                        "       sv STRING,                           "+
                        " 	    pt STRING>                           "+
                        " ) WITH(                                    "+
                        "     'connector' = 'hbase-2.2',             "+
                        "     'table-name' = 'dim_page_info',        "+
                        "     'zookeeper.quorum' = 'doitedu:2181'    "+
                        " )                                          "
        );


        // 6. 建表映射  hbase中的维表：地域信息表
        tenv.executeSql(
                " create table page_hbase(                        "+
                        "    url STRING,                             "+
                        "    f  ROW<                                 "+
                        "       p STRING,                           "+
                        "       c STRING,                           "+
                        " 	    r STRING>                           "+
                        " ) WITH(                                    "+
                        "     'connector' = 'hbase-2.2',             "+
                        "     'table-name' = 'dim_geo_area',        "+
                        "     'zookeeper.quorum' = 'doitedu:2181'    "+
                        " )                                          "
        );


        // 7. 进行关联






    }

}

package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时看板 指标计算任务
 *   今天每 5分钟的pv数、uv数、会话数
 *     2023-06-04 10:00:00,2023-06-04 10:05:00, 3259345,200203,178235
 *     2023-06-04 10:05:00,2023-06-04 10:10:00, 3259345,200203,178235
 *     2023-06-04 10:10:00,2023-06-04 10:15:00, 3259345,200203,178235
 */
public class Job1_PVUV_1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建映射表，映射 kafka中的 dwd_events
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
                        +"  'scan.startup.mode' = 'latest-offset',         "
                        +"  'value.format'='json',                           "
                        +"  'value.json.fail-on-missing-field'='false',      "
                        +"  'value.fields-include' = 'EXCEPT_KEY')           "
        );



        // 2. 创建映射表，映射mysql中的 流量看板表1
        tenv.executeSql(
                " CREATE TABLE dashboard_traffic_1 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   pv_amt   BIGINT,                      "
                        +"   uv_amt   BIGINT,                      "
                        +"   ses_amt   BIGINT                      "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/doit38',     "
                        +"    'table-name' = 'dashboard_traffic_1',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );



        // 3. 计算指标，并将结果输出到 目标存储
        // 2023-06-04 10:00:00,2023-06-04 10:05:00, 3259345,200203,178235
        // 逻辑： 开 5分钟 滚动窗口，在窗口内:
        // pv:sum(if事件=pageLoad,1,0) ,
        // uv: count(distinct user_id),
        // ses:count(distinct session_id)

        tenv.executeSql(
                " INSERT INTO dashboard_traffic_1                                       "+
                        " SELECT                                                           "+
                        "   window_start,                                                  "+
                        "   window_end,                                                    "+
                        "   sum(if(event_id='page_load',1,0)) as pv_amt,                   "+
                        "   count(distinct user_id) as uv_amt,                             "+
                        "   count(distinct session_id) as ses_amt                          "+
                        " from TABLE(                                                      "+
                        " TUMBLE(TABLE dwd_kafka,DESCRIPTOR(row_time),INTERVAL '5' MINUTE) "+
                        " )                                                                "+
                        " GROUP BY                                                         "+
                        "   window_start,                                                  "+
                        "   window_end                                                     "
        );

    }
}

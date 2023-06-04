package cn.doitedu.dashboard;

import cn.doitedu.udfs.GetNull;
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
public class Job3_PromotionQx {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/rtdw/ckpt");
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
                        +"  'scan.startup.mode' = 'latest-offset',           "
                        +"  'value.format'='json',                           "
                        +"  'value.json.fail-on-missing-field'='false',      "
                        +"  'value.fields-include' = 'EXCEPT_KEY')           "
        );



        // 2. 创建映射表，映射mysql中的 七夕活动流量实时看板表
        tenv.executeSql(
                " CREATE TABLE dashboard_promotion_qx_1 (            "
                        +"   promotion_page_name STRING,            "
                        +"   update_time TIMESTAMP(3),            "
                        +"   province STRING,                 "
                        +"   pv_amt   BIGINT,                      "
                        +"   uv_amt   BIGINT,                       "
                        +"   uv_amt_new   BIGINT                        "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/doit38',     "
                        +"    'table-name' = 'dashboard_promotion_qx_1',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );



        // 3. 计算指标，每5分钟更新一次，累计次此刻的七夕各活动页的来自各省份的pv总数和uv总数和新用户总数

        tenv.createTemporaryFunction("get_null", GetNull.class);
        tenv.executeSql(
                "INSERT into dashboard_promotion_qx_1   " +
                   " WITH tmp AS (                                                                                      "+
                        " SELECT                                                                                             "+
                        "    user_id,                                                                                        "+
                        "    case                                                                                            "+
                        "      when regexp( properties['url'],'/mall/promotion/qxlx.*?') then '七夕拉新活动页'               "+
                        "      when regexp( properties['url'],'/mall/promotion/qxcd.*?') then '七夕促单活动页'               "+
                        "      when regexp( properties['url'],'/mall/promotion/cmjh.*?') then '七夕激活活动页'               "+
                        "    end  as promotion_page_name,                                                                    "+
                        "    if(date_format(register_time,'yyyy-MM-dd')< DATE_FORMAT(row_time,'yyyy-MM-dd'),0,1) as is_new,  "+
                        "    gps_province as province,                                                                       "+
                        "    row_time                                                                                        "+
                        " FROM dwd_kafka                                                                                     "+
                        " WHERE event_id = 'page_load' AND                                                                   "+
                        " (                                                                                                  "+
                        "   regexp( properties['url'],'/mall/promotion/qxlx.*?')                                             "+
                        "     OR                                                                                             "+
                        "   regexp( properties['url'],'/mall/promotion/qxcd.*?')                                             "+
                        "     OR                                                                                             "+
                        "   regexp( properties['url'],'/mall/promotion/cmjh.*?')                                             "+
                        " )                                                                                                  "+
                        " )                                                                                                  "+
                        "                                                                                                    "+
                        " SELECT                                                                                             "+
                        "   promotion_page_name,                                                                             "+
                        "   window_end as update_time,                                                                       "+
                        "   province,                                                                                        "+
                        "   count(1) as pv_amt,                                                                              "+
                        "   count(distinct user_id) as uv_amt,                                                               "+
                        "   count(distinct if(is_new=1,user_id,get_null() )) as uv_amt_new                                          "+
                        "                                                                                                    "+
                        " from TABLE(                                                                                        "+
                        "  CUMULATE(TABLE tmp, DESCRIPTOR(row_time),INTERVAL '5' MINUTE,INTERVAL '24' HOUR)                  "+
                        " )                                                                                                  "+
                        " GROUP BY window_start,window_end,province,promotion_page_name                                      "

        );

    }
}

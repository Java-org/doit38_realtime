package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/7
 * @Desc: 学大数据，上多易教育
 *
 *  推荐栏位展示、点击行为olap分析，轻度聚合计算任务
 **/
public class Job6_RecommendOlapAggregate {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1. 读取行为明细数据
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



        // 2. 数据过滤及字段提取扁平化
        tenv.executeSql(
                " CREATE TEMPORARY VIEW rec_events AS  SELECT               "+
                        "   user_id,                                           "+
                        "   event_id,                                          "+
                        "   event_time,                                        "+
                        "   proc_time,                                         "+
                        "   properties['rec_tracking_id'] as rec_tracking_id,  "+
                        "   properties['rec_region_id'] as rec_region_id,      "+
                        "   page_type,                                         "+
                        "   device_type,                                       "+
                        "   row_time                                           "+
                        " FROM dwd_kafka                                       "+
                        " WHERE event_id in ('rec_show','rec_click')           "
        );



        // 3. 聚合计算
        /*
            数据规律:
            u1,rec_show,t1,rec_tk1,lw_01,pg01
            u1,rec_clik,t2,rec_tk1,lw_01,pg01
            u1,rec_clik,t4,rec_tk1,lw_01,pg01

            u1,rec_show,t6,rec_tk3,lw_02,pg03
            u1,rec_show,t7,rec_tk4,lw_02,pg03
            u1,rec_clik,t8,rec_tk4,lw_02,pg01
            u1,rec_clik,t9,rec_tk4,lw_02,pg01

            结果：
            u1,lw_01,曝光1次,点击2次
            u1,lw_02,曝光2次,点击2次

         */
        tenv.executeSql(
                " CREATE TEMPORARY VIEW  agg AS  SELECT                                       "
                        +"   user_id,                                                            "
                        +"   device_type,                                                        "
                        +"   rec_region_id,                                                      "
                        +"   page_type,                                                          "
                        +"   sum(if(event_id='rec_show',1,0)) as rec_show_cnt,                   "
                        +"   sum(if(event_id='rec_click',1,0)) as rec_click_cnt,                 "
                        +"   proctime() as proc_time                                             "
                        +" FROM TABLE (                                                           "
                        +"   TUMBLE(TABLE rec_events,DESCRIPTOR(row_time),INTERVAL '5' MINUTE)   "
                        +" )                                                                     "
                        +" GROUP BY                                                              "
                        +"   window_start,                                                       "
                        +"   window_end,                                                         "
                        +"   user_id,                                                            "
                        +"   device_type,                                                        "
                        +"   rec_region_id,                                                      "
                        +"   page_type                                                           "

        );

        // 4. 查维表得到推荐位的各种维度信息
        tenv.executeSql(
                " create table rec_info_hbase(                  " +
                        "    rec_region_id STRING,                 " +
                        "    f  ROW<                               " +
                        "      rec_name STRING,                    " +
                        "      rec_suan STRING,                    " +
                        "      rec_type STRING                     " +
                        " 	   >                                   " +
                        " ) WITH(                                  " +
                        "     'connector' = 'hbase-2.2',           " +
                        "     'table-name' = 'dim_rec_info',       " +
                        "     'zookeeper.quorum' = 'doitedu:2181'  " +
                        " )                                        "
        );

        tenv.executeSql(
                " CREATE TEMPORARY VIEW res AS  SELECT                                     "+
                        "  a.user_id,                                                          "+
                        "  a.device_type,                                                      "+
                        "  a.page_type,                                                        "+
                        "  a.rec_region_id,                                                    "+
                        "  r.f.rec_type as rec_region_type,                                    "+
                        "  r.f.rec_name as rec_region_name,                                    "+
                        "  r.f.rec_suan as rec_math_model,                                     "+
                        "  a.rec_show_cnt,                                                     "+
                        "  a.rec_click_cnt                                                     "+
                        "                                                                      "+
                        " FROM agg a                                                           "+
                        " LEFT JOIN rec_info_hbase FOR SYSTEM_TIME AS OF a.proc_time AS r      "+
                        " ON a.rec_region_id = r.rec_region_id                                 "
        );


        // 5.创建doris目标映射表
        tenv.executeSql(
                "CREATE TABLE sink_doris(             "+
                        " user_id           BIGINT,   "+
                        " device_type       STRING,   "+
                        " page_type         STRING,   "+
                        " rec_region_id     STRING,   "+
                        " rec_region_type   STRING,   "+
                        " rec_region_name   STRING,   "+
                        " rec_math_model    STRING,   "+
                        " rec_show_cnt     BIGINT ,     "+
                        " rec_click_cnt    BIGINT       "+
                        ") WITH (                                        "+
                        "   'connector' = 'doris',                       "+
                        "   'fenodes' = 'doitedu:8030',                  "+
                        "   'table.identifier' = 'dws.recommend_ana_agg',   "+
                        "   'username' = 'root',                         "+
                        "   'password' = 'root',                         "+
                        "   'sink.label-prefix' = 'doris_label92'        "+
                        ")                                               "
        );

        // 6.写入结果
        tenv.executeSql("insert into sink_doris select * from res ");

    }
}

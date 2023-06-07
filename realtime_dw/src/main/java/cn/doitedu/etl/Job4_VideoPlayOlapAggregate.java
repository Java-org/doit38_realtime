package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/6
 * @Desc: 学大数据，上多易教育
 * <p>
 * 视频播放行为主题olap分析，轻度聚合任务
 **/
public class Job4_VideoPlayOlapAggregate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建映射表，映射kafka明细层的行为日志
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


        // 2. 创建hbase中的视频信息维表映射
        tenv.executeSql(
                " create table video_hbase(                     " +
                        "    id BIGINT,                            " +
                        "    f  ROW<                               " +
                        "      video_name STRING,                  " +
                        "      video_type STRING,                  " +
                        "      video_album STRING,                 " +
                        "      video_author STRING,                " +
                        "      video_timelong bigint,              " +
                        "      create_time timestamp(3),           " +
                        "      update_time timestamp(3)            " +
                        " 	   >                                   " +
                        " ) WITH(                                  " +
                        "     'connector' = 'hbase-2.2',           " +
                        "     'table-name' = 'dim_video_info',     " +
                        "     'zookeeper.quorum' = 'doitedu:2181'  " +
                        " )                                        "

        );


        // 3. 创建 doris中的目标聚合表的映射
        tenv.executeSql(
                "CREATE TABLE sink_doris (                  " +
                        "  start_dt         DATE,              " +
                        "  user_id          BIGINT,            " +
                        "  release_channel  STRING,            " +
                        "  device_type      STRING,            " +
                        "  video_id         BIGINT ,           " +
                        "  video_play_id    STRING,            " +
                        "  video_name       STRING,            " +
                        "  video_type       STRING,            " +
                        "  video_album      STRING,            " +
                        "  video_author     STRING,            " +
                        "  video_timelong   BIGINT,            " +
                        "  create_time      TIMESTAMP(3),      " +
                        "  play_start_time  BIGINT ,           " +
                        "  play_end_time    BIGINT             " +
                        ") WITH (                                           " +
                        "   'connector' = 'doris',                          " +
                        "   'fenodes' = 'doitedu:8030',                     " +
                        "   'table.identifier' = 'dws.video_events_agg01',  " +
                        "   'username' = 'root',                            " +
                        "   'password' = 'root',                            " +
                        "   'sink.label-prefix' = 'doris_label99'           " +
                        ")                                                  "
        );

        /*
          数据的规律
          u1,p01,video_play,t01,v01
          u1,p01,video_heat,t02,v01
          u1,p01,video_heat,t03,v01
          u1,p01,video_heat,t04,v01
          u1,p01,video_paus,t05,v01  暂停

          u1,p01,video_resu,t20,v01  继续
          u1,p01,video_heat,t25,v01
          u1,p01,video_heat,t30,v01
          u1,p01,video_stop,t32,v01

         要计算的结果：
         用户，维度，playId,起始时间,结束时间
         u1,  -   ,p01   ,t01      t05
         u1,  -   ,p01   ,t20      t32
      */

        // 第1步，事件过滤及抽取properties字段扁平化
        tenv.executeSql(
                " CREATE TEMPORARY VIEW tmp AS    select                                                      "+
                        "     user_id         ,                                                                  "+
                        "     release_channel ,                                                                  "+
                        "     device_type     ,                                                                  "+
                        "     properties['play_id']  as  video_play_id,                                          "+
                        "     CAST(properties['video_id'] AS BIGINT) as  video_id  ,                             "+
                        " 	  event_id,                                                                          "+
                        " 	  event_time,                                                                        "+
                        "     row_time	                                                                         "+
                        " from dwd_kafka                                                                         "+
                        " where event_id in ('video_play','video_hb','video_pause','video_resume','video_stop')  "
        );



        // 第2步，打标记便于将来分组
        // 每条数据加一个flag字段，普通事件为0，resume事件为1
        // 然后 sum over 这个flag字段，得到分组标记new_flag字段
        tenv.executeSql(
                " CREATE TEMPORARY VIEW o2 AS                                                 "
                        +" SELECT                                                                      "
                        +"     user_id         ,                                                       "
                        +"     release_channel ,                                                       "
                        +"     device_type     ,                                                       "
                        +"     video_play_id,                                                          "
                        +"     video_id  ,                                                             "
                        +" 	event_id,                                                                  "
                        +" 	event_time,                                                                "
                        +" 	row_time,                                                                  "
                        +" 	sum(flag) over(partition by user_id,video_play_id order by row_time rows   "
                        +" 	  between unbounded preceding and current row) as new_flag                 "
                        +"                                                                             "
                        +" FROM                                                                        "
                        +" (                                                                           "
                        +"     SELECT                                                                  "
                        +"         user_id         ,                                                   "
                        +"         release_channel ,                                                   "
                        +"         device_type     ,                                                   "
                        +"         video_play_id,                                                      "
                        +"         video_id  ,                                                         "
                        +"     	event_id,                                                              "
                        +"     	event_time,                                                            "
                        +"     	row_time,                                                              "
                        +"         if(event_id = 'video_resume' , 1, 0) as flag	                       "
                        +"     FROM tmp                                                                "
                        +" ) o1                                                                        "
        );

        // 按时间窗口，在时间窗口内进行分组聚合
        tenv.executeSql(
                "CREATE TEMPORARY VIEW agg AS   SELECT                                       "+
                        "   to_date(date_format(to_timestamp_ltz(min(event_time),3),'yyyy-MM-dd')) as start_dt"+
                        "   ,user_id                                                     "+
                        "    ,release_channel                                             "+
                        "    ,device_type                                                 "+
                        "    ,video_play_id                                               "+
                        "    ,video_id                                                    "+
                        "    ,new_flag                                                    "+
                        "	,min(event_time) as start_time                                "+
                        "	,max(event_time) as end_time                                  "+
                        "	,proctime() as proc_time                                      "+
                        "                                                                 "+
                        "FROM TABLE(                                                      "+
                        " TUMBLE(TABLE o2,DESCRIPTOR(row_time),INTERVAL '5' MINUTE)       "+
                        ")                                                                "+
                        "GROUP BY                                                         "+
                        "    window_start                                                 "+
                        "    ,window_end                                                  "+
                        "    ,user_id                                                     "+
                        "    ,release_channel                                             "+
                        "    ,device_type                                                 "+
                        "    ,video_play_id                                               "+
                        "    ,video_id                                                    "+
                        "    ,new_flag                                                    "
        );

        // 关联视频维表信息，并写入doris
        tenv.executeSql(
                " INSERT INTO sink_doris                                           "+
                        " SELECT                                                           "+
                        "  start_dt                                                        "+
                        "  ,user_id                                                        "+
                        "  ,release_channel                                                "+
                        "  ,device_type                                                    "+
                        "  ,video_id                                                       "+
                        "  ,video_play_id                                                  "+
                        "  ,video_name                                                     "+
                        "  ,video_type                                                     "+
                        "  ,video_album                                                    "+
                        "  ,video_author                                                   "+
                        "  ,video_timelong                                                 "+
                        "  ,create_time                                                    "+
                        "  ,start_time                                                     "+
                        "  ,end_time                                                       "+
                        " FROM  agg                                                        "+
                        " left join video_hbase FOR SYSTEM_TIME AS OF agg.proc_time AS v   "+
                        " ON agg.video_id = v.id                                           "
        );
    }
}

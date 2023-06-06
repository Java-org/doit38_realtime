package cn.doitedu.sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/6
 * @Desc: 学大数据，上多易教育
 *
 *   业务库的视频信息表，同步到hbase
 **/
public class VideoTable2Hbase {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/xx/yy/ckpt/");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建cdc连接器表，映射业务库中的 视频信息表
        tenv.executeSql(
                "CREATE TABLE video_mysql (    " +
                        "      id BIGINT," +
                        "      video_name STRING,                      " +
                        "      video_type STRING,                      " +
                        "      video_album STRING,                     " +
                        "      video_author STRING,                     " +
                        "      video_timelong bigint,                  " +
                        "      create_time timestamp(3),               " +
                        "      update_time timestamp(3),               " +
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'realtimedw',          " +
                        "     'table-name' = 'cms_video'              " +
                        ")"
        );

        // 创建hbase连接器表，映射 hbase中 dim_video_info 表
        tenv.executeSql(
                " create table video_hbase(                   "+
                        "    id BIGINT,                          "+
                        "    f  ROW<                                 "+
                        "      video_name STRING,                      " +
                        "      video_type STRING,                      " +
                        "      video_album STRING,                     " +
                        "      video_author STRING,                     " +
                        "      video_timelong bigint,                  " +
                        "      create_time timestamp(3),               " +
                        "      update_time timestamp(3)               " +
                        " 	   >                                     "+
                        " ) WITH(                                    "+
                        "     'connector' = 'hbase-2.2',             "+
                        "     'table-name' = 'dim_video_info',        "+
                        "     'zookeeper.quorum' = 'doitedu:2181'    "+
                        " )                                          "

        );

        // insert .. select ....
        tenv.executeSql("insert into video_hbase SELECT id,row(video_name,video_type,video_album,video_author,video_timelong,create_time,update_time) from video_mysql");

    }
}

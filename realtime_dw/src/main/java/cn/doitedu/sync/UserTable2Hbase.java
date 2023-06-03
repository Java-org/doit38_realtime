package cn.doitedu.sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 数据同步任务：
 *    业务库用户注册信息表  -->  hbase
 */
public class UserTable2Hbase {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/xx/yy/ckpt/");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建一个逻辑表 user_mysql，映射 mysql中的表，用cdc连接器
        tenv.executeSql(
                "CREATE TABLE user_mysql (    " +
                        "      id BIGINT," +
                        "      username STRING,                        " +
                        "      phone STRING,                           " +
                        "      status int,                             " +
                        "      create_time timestamp(3),               " +
                        "      gender int,                             " +
                        "      birthday date,                          " +
                        "      province STRING,                        " +
                        "      city STRING,                            " +
                        "      job STRING ,                            " +
                        "      source_type INT ,                       " +
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'realtimedw',          " +
                        "     'table-name' = 'ums_member'              " +
                        ")"
        );


        // 2.创建一个逻辑表 user_hbase，映射hbase中的表，用 hbase-connector连接器

        tenv.executeSql(
                        " create table user_hbase(                   "+
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


        // 3. 写一个sql，从user_mysql读数据，insert到 user_hbase
        tenv.executeSql("insert into user_hbase select username,row(id,phone,status,create_time,gender,birthday,province,city,job,source_type) as f1 from user_mysql");

    }

}

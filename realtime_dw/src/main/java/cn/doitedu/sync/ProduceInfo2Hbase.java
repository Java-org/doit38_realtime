package cn.doitedu.sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProduceInfo2Hbase {
    public static void main(String[] args) {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表，映射mysql中的商品信息表
        tenv.executeSql(
                "CREATE TABLE mysql_product (    " +
                        "      id BIGINT," +
                        "      brand_id BIGINT," +
                        "      product_category_id BIGINT," +
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'realtimedw',          " +
                        "     'table-name' = 'pms_product'             " +
                        ")"
        );


        // 建表，映射hbase中的目标表
        tenv.executeSql(
                " create table dim_product(                        " +
                        "    id STRING,                               " + //   反转(Long.MAX - 商品id)
                        "    f  ROW<brand_id BIGINT,product_category_id BIGINT>      " +
                        " ) WITH(                                     " +
                        "     'connector' = 'hbase-2.2',              " +
                        "     'table-name' = 'dim_product',           " +
                        "     'zookeeper.quorum' = 'doitedu:2181'     " +
                        " )                                           "
        );


        // 同步逻辑
        tenv.executeSql("insert into dim_product select reverse(cast(10000000000-id as string)), row(brand_id,product_category_id) from  mysql_product");


    }

}

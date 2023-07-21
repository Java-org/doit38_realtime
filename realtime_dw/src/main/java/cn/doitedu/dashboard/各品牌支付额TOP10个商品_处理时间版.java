package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class 各品牌支付额TOP10个商品_处理时间版 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 订单主表
        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        "      id BIGINT," +
                        "      status INT,                        " +
                        "      create_time timestamp(3),          " +
                        "      modify_time timestamp(3),          " +
                        "      payment_time timestamp(3),         " +
                        "     PRIMARY KEY (id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'realtimedw',     " +
                        "     'table-name' = 'oms_order'          " +
                        ")"
        );

        // 订单详情表
        tenv.executeSql(
                "CREATE TABLE item_mysql (    " +
                        "      id       BIGINT,  " +
                        "      order_id BIGINT,  " +
                        "      product_id BIGINT,                 " +
                        "      product_brand STRING,              " +
                        "      real_amount decimal(10,2),       " +
                        "     PRIMARY KEY (id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'realtimedw',     " +
                        "     'table-name' = 'oms_order_item'     " +
                        ")"
        );


        // 关联
        tenv.executeSql(
                " CREATE TEMPORARY VIEW joined AS  SELECT  " +
                        "    o.id as order_id,                " +
                        "    o.create_time ,                  " +
                        "    o.payment_time,                  " +
                        "    o.status,                        " +
                        "    i.product_id,                    " +
                        "    i.product_brand,                 " +
                        "    i.real_amount,                   " +
                        "    proctime() as pt                 " +
                        " from order_mysql o                  " +
                        " join item_mysql i                   " +
                        " on o.id = i.order_id                 "
        );


        // 窗口聚合出 每个品牌金额最大的前10个商品
        tenv.executeSql(
                "SELECT\n" +
                        "    window_start,\n" +
                        "    window_end,\n" +
                        "    product_brand,\n" +
                        "    product_id,\n" +
                        "    real_amount,\n" +
                        "    row_number() over(PARTITION BY window_start,window_end,product_brand ORDER BY pt asc,real_amount DESC) as rn \n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        TUMBLE_START(pt,INTERVAL '24' HOUR) as window_start,\n" +
                        "        TUMBLE_END(pt,INTERVAL '24' HOUR) as window_end,\n" +
                        "        product_brand,\n" +
                        "        product_id,\n" +
                        "        proctime() as pt,\n" +
                        "        sum(real_amount) as real_amount \n" +
                        "    from joined \n" +
                        "    GROUP BY \n" +
                        "        TUMBLE(pt,INTERVAL '24' HOUR),\n" +
                        "        product_brand,\n" +
                        "        product_id\n" +
                        ") o \n"

        ).print();


    }

}

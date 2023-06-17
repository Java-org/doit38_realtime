package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job8_OrderItemSyncDoris {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 限定本job中join的状态ttl
        tenv.getConfig().set("table.exec.state.ttl","360 hour");

        // 1. 建表映射 业务库中的  oms_order表
        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        " id            INT ,             "+
                        " status        INT ,             "+
                        " total_amount  decimal(10,0) ,   "+
                        " pay_amount    decimal(10,0) ,   "+
                        " create_time   timestamp(3)  ,   "+
                        " payment_time  timestamp(3)  ,   "+
                        " delivery_time timestamp(3)  ,   "+
                        " confirm_time  timestamp(3)  ,   "+
                        " note          STRING ,          "+
                        " update_time   timestamp(3)  ,   "+
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'rtmk' ,               " +
                        "     'table-name' = 'oms_order'               " +
                        ")"
        );



        // 2. 建表映射 业务库中的 oms_order_id表
        tenv.executeSql(
                "CREATE TABLE item_mysql (           " +
                        "     id       INT,             "+
                        "     oid      INT,             "+
                        "     pid      INT,             "+
                        "     price    DECIMAL(10,0),   "+
                        "     brand    STRING,          "+
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'rtmk' ,               " +
                        "     'table-name' = 'oms_order_item'          " +
                        ")"
        );


        // 3. 建表映射 doris中的目标表  dwd.order_item_detail
        tenv.executeSql(
                "CREATE TABLE sink_doris(              "+
                        " oid           INT ,             "+
                        " id       INT,                   "+
                        " pid      INT,                   "+
                        " status        INT ,             "+
                        " total_amount  decimal(10,0) ,   "+
                        " pay_amount    decimal(10,0) ,   "+
                        " create_time   timestamp(3)  ,   "+
                        " payment_time  timestamp(3)  ,   "+
                        " delivery_time timestamp(3)  ,   "+
                        " confirm_time  timestamp(3)  ,   "+
                        " note          STRING ,          "+
                        " update_time   timestamp(3)  ,   "+
                        " price    DECIMAL(10,0),         "+
                        " brand    STRING  ,              "+
                        " PRIMARY KEY (oid,id,pid) NOT ENFORCED     " +
                        ") WITH (                                        "+
                        "   'connector' = 'doris',                       "+
                        "   'fenodes' = 'doitedu:8030',                  "+
                        "   'table.identifier' = 'dwd.oms_order_item_detail',   "+
                        "   'username' = 'root',                         "+
                        "   'password' = 'root',                         "+
                        "   'sink.label-prefix' = 'doris_label"+System.currentTimeMillis()+"'        "+
                        ")                                               "
        );

        // 4. 关联之后写 doris
        tenv.executeSql(
                " INSERT INTO sink_doris    "+
                        " SELECT                    "+
                        "     o.id  as oid          "+
                        "     ,i.id                 "+
                        "     ,i.pid                "+
                        "     ,o.status             "+
                        "     ,o.total_amount       "+
                        "     ,o.pay_amount         "+
                        "     ,o.create_time        "+
                        "     ,o.payment_time       "+
                        "     ,o.delivery_time      "+
                        "     ,o.confirm_time       "+
                        "     ,o.note               "+
                        "     ,o.update_time        "+
                        "     ,i.price              "+
                        "     ,i.brand              "+
                        " FROM order_mysql o        "+
                        " JOIN item_mysql i         "+
                        " ON o.id = i.oid           "
        );
    }
}

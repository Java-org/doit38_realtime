package bak;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Job5_OrderDaySettlement_Sql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        "      id BIGINT," +
                        "      status INT,                    " +
                        "      payment_time timestamp(3),     " +
                        "      pt as proctime()     ,         " +
                        "      rt as payment_time        ,         " +
                        "      watermark for rt as rt - interval '0' second ,  " +
                        "     PRIMARY KEY (id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'rtmk',           " +
                        "     'table-name' = 'oms_order'          " +
                        ")"
        );

        tenv.executeSql(
                "CREATE TABLE item_mysql (    " +
                        "      id BIGINT," +
                        "      oid BIGINT,                    " +
                        "      product_id BIGINT,             " +
                        "      product_price decimal(10,2),   " +
                        "      product_quantity INT,          " +
                        "      product_brand STRING,              " +
                        "     PRIMARY KEY (id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'rtmk',           " +
                        "     'table-name' = 'oms_order_item'          " +
                        ")"
        );

        // 订单主表 和 订单商品项 关联
        tenv.executeSql(
                " create TEMPORARY view joined as           "+
                        " with od as (                              "+
                        " select *                                  "+
                        " from                                      "+
                        " order_mysql                               "+
                        " where                                     "+
                        "   payment_time is not null                     "+
                        "   and (status=1 or status=2 or status=3 ) "+
                        " )                                         "+
                        " SELECT                                    "+
                        "   od.id AS oid,                           "+
                        "   od.status,                              "+
                        "   od.payment_time,                             "+
                        "   it.*                                    "+
                        " FROM  od                                  "+
                        " LEFT JOIN                                 "+
                        " item_mysql it                             "+
                        " ON od.id = it.oid                         "
                );


        /**
         * 对关联后的数据
         * 通过表->流，再 流-> 表，重新定义rowtime
         */
        Schema schema = Schema.newBuilder()
                .column("oid", DataTypes.BIGINT())
                .column("status", DataTypes.INT())
                .column("payment_time", DataTypes.TIMESTAMP(3))
                .column("id", DataTypes.BIGINT())
                .column("product_id", DataTypes.BIGINT())
                .column("product_price", DataTypes.DECIMAL(10,2))
                .column("product_quantity", DataTypes.INT())
                .column("product_brand", DataTypes.STRING())
                .columnByExpression("rt", "payment_time")
                .watermark("rt", "rt - interval '0' second")
                .build();
        DataStream<Row> ds = tenv.toChangelogStream(tenv.from("joined"));
        Table table = tenv.fromChangelogStream(ds, schema);

        tenv.createTemporaryView("timed",table);

        /**
         * 利用 groupByWindow  和 row_number  计算 窗口内各品牌成交额topn商品
         */
        tenv.executeSql(
                "  create temporary view res as                                                  "+
                        "  with tmp as (                                                                 "+
                        "    SELECT                                                                      "+
                        "        tumble_start(rt,interval '1' minute) as w_start                         "+
                        "        ,tumble_end(rt,interval '1' minute) as w_end                            "+
                        "        ,tumble_rowtime(rt,interval '1' minute) as rt                           "+
                        "        ,product_brand,                                                         "+
                        "        ,product_id,                                                            "+
                        "        ,sum(product_price * product_quantity) as product_amount                "+
                        "    from timed                                                                  "+
                        "    group by                                                                    "+
                        "    tumble(rt,interval '1' minute)                                              "+
                        "    ,product_brand                                                              "+
                        "    ,product_id                                                                 "+
                        "  )                                                                             "+
                        "                                                                                "+
                        " SELECT * FROM (                                                                "+
                        " SELECT                                                                         "+
                        "   w_start,                                                                     "+
                        "   w_end,                                                                       "+
                        "   product_brand,                                                               "+
                        "   product_id,                                                                  "+
                        "   product_amount,                                                              "+
                        "   row_number() over(partition by w_start,w_end,product_brand order by product_amount desc) as rn "+
                        " from tmp ) WHERE rn<=2                                                         "
                );


        /* *
         *  创建 mysql中的结果表映射 ------------------------------------------
         */
        tenv.executeSql(
                " CREATE TABLE mysql_sink (                      "
                        +"   window_start timestamp(3),                      "
                        +"   window_end timestamp(3),                        "
                        +"   product_brand     STRING,                       "
                        +"   product_id   BIGINT,                            "
                        +"   product_pay_amount   DECIMAL(10,2),             "
                        +"   rn   BIGINT,                                    "
                        +"   PRIMARY KEY (window_start,window_end,product_brand,product_id) NOT ENFORCED "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/rtmk',      "
                        +"    'table-name' = 'dashboard_product_brand_topn_item',    "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );

        /**
         * 输出最终结果
         */
        tenv.executeSql("insert into mysql_sink select * from  res");


        env.execute();
    }

}

package cn.doitedu.dashboard;

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
                        "      paytime timestamp(3),          " +
                        "      other STRING,                  " +
                        "      pt as proctime()     ,         " +
                        "      rt as paytime        ,         " +
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
                        "      pid BIGINT,                    " +
                        "      price decimal(10,2),          " +
                        "      brand STRING,                  " +
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
                        "   paytime is not null                     "+
                        "   and (status=1 or status=2 or status=3 ) "+
                        " )                                         "+
                        " SELECT                                    "+
                        "   od.id AS oid,                           "+
                        "   od.status,                              "+
                        "   od.paytime,                             "+
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
                .column("paytime", DataTypes.TIMESTAMP(3))
                .column("id", DataTypes.BIGINT())
                .column("pid", DataTypes.BIGINT())
                .column("price", DataTypes.DECIMAL(10,2))
                .column("brand", DataTypes.STRING())
                .columnByExpression("rt", "paytime")
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
                        "        ,brand,                                                                 "+
                        "        ,pid,                                                                   "+
                        "        ,sum(price) as pamt                                                     "+
                        "    from timed                                                                  "+
                        "    group by                                                                    "+
                        "    tumble(rt,interval '1' minute)                                              "+
                        "    ,brand                                                                      "+
                        "    ,pid                                                                        "+
                        "  )                                                                             "+
                        "                                                                                "+
                        " SELECT * FROM (                                                                "+
                        " SELECT                                                                         "+
                        "   w_start,                                                                     "+
                        "   w_end,                                                                       "+
                        "   brand,                                                                       "+
                        "   pid,                                                                         "+
                        "   pamt,                                                                        "+
                        "   row_number() over(partition by w_start,w_end,brand order by pamt desc) as rn "+
                        " from tmp ) WHERE rn<=2                                                         "
                );


        /* *
         *  创建mysql中的结果表映射 ------------------------------------------
         */
        tenv.executeSql(
                " CREATE TABLE mysql_sink (                      "
                        +"   window_start timestamp(3),                      "
                        +"   window_end timestamp(3),                        "
                        +"   brand     STRING,                               "
                        +"   product_id   BIGINT,                            "
                        +"   product_pay_amount   DECIMAL(10,2),             "
                        +"   rn   BIGINT,                                    "
                        +"   PRIMARY KEY (window_start,window_end,brand,product_id) NOT ENFORCED "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/rtmk',      "
                        +"    'table-name' = 'dashboard_brand_topn_item',    "
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

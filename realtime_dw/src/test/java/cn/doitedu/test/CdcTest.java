package cn.doitedu.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class CdcTest {
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

        /*tenv.executeSql("SELECT\n" +
                "  od.*,\n" +
                "  it.*\n" +
                "FROM ( \n" +
                "SELECT\n" +
                " *\n" +
                "FROM TABLE(\n" +
                " TUMBLE(TABLE order_mysql,DESCRIPTOR(rt),INTERVAL '1' MINUTE)\n" +
                ")  ) od\n" +
                "\n" +
                "LEFT JOIN  item_mysql it  ON od.id = it.oid").print();*/


        tenv.executeSql(
                "create TEMPORARY view joined as \n" +
                "with od as (\n" +
                "select * from order_mysql where paytime is not null and (status=1 and other = 'a' ) or (status=2 and other = 'a' ) or (status=3 and other = 'a' ) \n" +
                ") \n" +
                "SELECT\n" +
                "  od.id AS oid,\n" +
                "  od.status,\n" +
                "  od.paytime,\n" +
                "  od.other,\n" +
                "  it.*\n" +
                "FROM  od  LEFT JOIN item_mysql it ON od.id = it.oid").print();


        Schema schema = Schema.newBuilder()
                .column("oid", DataTypes.BIGINT())
                .column("status", DataTypes.INT())
                .column("paytime", DataTypes.TIMESTAMP(3))
                .column("other", DataTypes.STRING())
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
        tenv.executeSql("select * from timed");


        /*tenv.executeSql("SELECT\n" +
                "tumble_start(rt,interval '1' minute) as window_start,\n" +
                "tumble_end(rt,interval '1' minute) as window_end,\n" +
                "tumble_rowtime(rt,interval '1' minute) as rt,\n" +
                "brand,pid,sum(price) as pamt\n" +
                "from timed\n" +
                "group by \n" +
                "tumble(rt,interval '1' minute)\n" +
                ",brand,pid").print();*/

        tenv.executeSql(
                "create temporary view res as " +
                "with tmp as (\n" +
                "SELECT\n" +
                "tumble_start(rt,interval '1' minute) as w_start,\n" +
                "tumble_end(rt,interval '1' minute) as w_end,\n" +
                "tumble_rowtime(rt,interval '1' minute) as rt,\n" +
                "brand,pid,sum(price) as pamt\n" +
                "from timed\n" +
                "group by \n" +
                "tumble(rt,interval '1' minute)\n" +
                ",brand,pid\n" +
                ")\n" +
                "\n" +
                "SELECT * FROM (\n" +
                "SELECT\n" +
                "  w_start,\n" +
                "  w_end,\n" +
                "  brand,\n" +
                "  pid,\n" +
                "  pamt,\n" +
                "  row_number() over(partition by w_start,w_end,brand order by pamt desc) as rn\n" +
                "from tmp ) WHERE rn<=2");


        /**
         *  ------------------------------------------
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
        tenv.executeSql("insert into mysql_sink select * from  res");





        env.execute();


    }

}

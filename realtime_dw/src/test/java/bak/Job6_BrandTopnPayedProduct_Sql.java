package bak;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/10
 * @Desc: 学大数据，上多易教育
 *
 *   实时看板指标： 每小时 ，每个品牌中， 已支付金额最大的前 N个商品
 **/
public class Job6_BrandTopnPayedProduct_Sql {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        "      id BIGINT," +
                        "      status INT,                        " +
                        "      create_time timestamp(3),          " +
                        "      modify_time timestamp(3),          " +
                        "      payment_time timestamp(3),         " +
                        "      rt as modify_time        ,         " +
                        "      watermark for rt as rt - interval '0' second ,  " +
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


        /*tenv.executeSql("create temporary view payed_order AS " +
                "select * from order_mysql " +
                "where date_format(create_time,'yyyy-MM-dd') = CURRENT_DATE  " +
                "and payment_time is not null");*/

        // 将 表转流，再转表 （趁此机会定义watermark）
        DataStream<Row> payedOrderChangelogStream = tenv.toChangelogStream(tenv.from("payed_order"));

        // 带变化语义的流，转成  临时视图时，本写法支持
        Table table = tenv.fromChangelogStream(payedOrderChangelogStream,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("status", DataTypes.INT())
                        .column("create_time", DataTypes.TIMESTAMP(3))
                        .column("payment_time", DataTypes.TIMESTAMP(3))
                        .columnByExpression("rt", "payment_time")
                        .watermark("rt", "rt - interval '0' second ")
                        .build());
        tenv.createTemporaryView("rt_payed_order",table);

        // 带变化语义的流，转成  临时视图时，本写法不支持！
        /*tenv.createTemporaryView("rt_payed_order",payedOrderChangelogStream,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("status", DataTypes.INT())
                        .column("create_time", DataTypes.TIMESTAMP(3))
                        .column("payment_time", DataTypes.TIMESTAMP(3))
                        .columnByExpression("rt","payment_time")
                        .watermark("rt","rt - interval '0' second ")
                        .build());*/

        //tenv.executeSql("select * from rt_payed_order ").print();


        tenv.executeSql(
                "CREATE TABLE item_mysql (    " +
                        "      id       BIGINT,  " +
                        "      order_id BIGINT,  " +
                        "      product_id BIGINT,                 " +
                        "      product_brand STRING,              " +
                        "      product_quantity INT,              " +
                        "      product_price decimal(10,2),       " +
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


        // 关联  订单主表  和  订单商品项表
        tenv.executeSql(
                "create temporary view tmp  as  SELECT\n" +
                        "  it.order_id,\n" +
                        "  it.product_id,\n" +
                        "  it.product_brand,\n" +
                        "  it.product_quantity,\n" +
                        "  it.product_price,\n" +
                        "  od.status,\n" +
                        "  od.create_time,\n" +
                        "  od.payment_time,\n" +
                        "  od.rt  \n" +
                        "FROM  item_mysql it \n" +
                        "LEFT JOIN  order_mysql od\n" +
                        "ON it.order_id = od.id \n" +
                        "where od.status is not null \n"
        );

        tenv.executeSql("desc tmp").print();


        tenv.executeSql("with agg AS (\n" +
                "select\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    product_brand,\n" +
                "    product_id,\n" +
                "    sum(product_quantity * product_price) as product_pay_amount\n" +
                "from TABLE(\n" +
                "  TUMBLE(TABLE tmp,DESCRIPTOR(rt),INTERVAL '5' MINUTE)\n" +
                ")\n" +
                "GROUP BY \n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    product_brand,\n" +
                "    product_id\n" +
                ")\n" +
                "\n" +
                "SELECT\n" +
                "\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    product_brand,\n" +
                "    product_id,\n" +
                "\tproduct_pay_amount,\n" +
                "\trn\n" +
                "\n" +
                "FROM (\n" +
                "select\n" +
                "\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    product_brand,\n" +
                "    product_id,\n" +
                "\tproduct_pay_amount,\n" +
                "\trow_number() over(partition by window_start,window_end,product_brand order by product_pay_amount desc) as rn\n" +
                "\n" +
                "from agg ) o \n" +
                "WHERE rn<=1\n").print();





    }

}

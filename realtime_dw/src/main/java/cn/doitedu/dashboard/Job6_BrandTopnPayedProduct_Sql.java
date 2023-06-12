package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/10
 * @Desc: 学大数据，上多易教育
 * <p>
 * 实时看板指标： 每小时 ，每个品牌中， 已支付金额最大的前 N个商品
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
                        "      rt as payment_time        ,         " +
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


        tenv.executeSql(
                "CREATE TABLE item_mysql (    " +
                        "      id       BIGINT,  " +
                        "      order_id BIGINT,  " +
                        "      product_id BIGINT,                 " +
                        "      product_name STRING,               " +
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

        // join后，丢失rowtime属性
        tenv.executeSql("CREATE TEMPORARY VIEW joined AS SELECT " +
                "  od.id, " +
                "  od.status, " +
                "  od.create_time, " +
                "  od.modify_time, " +
                "  od.payment_time, " +
                "  od.rt, " +
                "  it.product_id, " +
                "  it.product_name, " +
                "  it.product_brand, " +
                "  it.product_quantity * product_price as price " +
                "FROM (  " +
                "SELECT " +
                "  * " +
                "from order_mysql " +
                "where status in (1,2,3) and  date_format(payment_time,'yyyy-MM-dd') = CURRENT_DATE  " +
                ") od " +
                "LEFT JOIN item_mysql it " +
                "ON od.id = it.order_id");

        //tenv.executeSql("select * from joined").print();


        // 表转流，流转表 => 设法重设 rowtime
        Table joinedTable = tenv.from("joined");
        DataStream<Row> changelogStream = tenv.toChangelogStream(joinedTable);
        /*tenv.createTemporaryView("tmp", changelogStream,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("status", DataTypes.INT())
                        .column("payment_time", DataTypes.TIMESTAMP(3))
                        .column("price", DataTypes.DECIMAL(10, 2))
                        .column("product_id", DataTypes.BIGINT())
                        .column("product_brand", DataTypes.STRING())
                        .column("rt", DataTypes.TIMESTAMP(3))
                        .watermark("rt", "rt - interval '0' second")
                        .build());*/

        Table table2 = tenv.fromChangelogStream(changelogStream,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("status", DataTypes.INT())
                        .column("payment_time", DataTypes.TIMESTAMP(3))
                        .column("price", DataTypes.DECIMAL(10, 2))
                        .column("product_id", DataTypes.BIGINT())
                        .column("product_name", DataTypes.STRING())
                        .column("product_brand", DataTypes.STRING())
                        .column("rt", DataTypes.TIMESTAMP(3))
                        .watermark("rt", "rt - interval '0' second")
                        .build());
        tenv.createTemporaryView("tmp",table2);

        // StreamPhysicalWindowAggregate doesn't support consuming update and delete changes
        tenv.executeSql("SELECT " +
                "window_start,window_end,product_brand, " +
                "sum(price) as amount " +
                "FROM TABLE( " +
                "  TUMBLE(TABLE tmp , DESCRIPTOR(rt) , INTERVAL '10' MINUTE)\n" +
                ") " +
                "GROUP BY window_start,window_end,product_brand").print();

        // 虽然能够运行，但是结果也不正确
        tenv.executeSql("SELECT " +
                "tumble_start(rt,interval '10' minute) as window_start, " +
                "product_brand,product_id,product_name,  " +
                "sum(price) as amount " +
                "from tmp " +
                "group by  " +
                "tumble(rt,interval '10' minute), " +
                "product_brand,product_id,product_name").print();





    }

}

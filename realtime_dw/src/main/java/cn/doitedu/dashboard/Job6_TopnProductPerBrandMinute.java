package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.types.Row.withNames;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/17
 * @Desc: 学大数据，上多易教育
 * <p>
 * 每分钟各品牌支付额最高的topn商品  -- 实时看板
 **/
public class Job6_TopnProductPerBrandMinute {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 限定本job中join的状态ttl
        tenv.getConfig().set("table.exec.state.ttl", "360 hour");

        // 1. 建表映射 业务库中的  oms_order表
        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        " id            INT ,             " +
                        " status        INT ,             " +
                        " payment_time  timestamp(3)  ,   " +
                        " update_time   timestamp(3)  ,   " +
                        " PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                             " +
                        "     'connector' = 'mysql-cdc',           " +
                        "     'hostname' = 'doitedu'   ,           " +
                        "     'port' = '3306'          ,           " +
                        "     'username' = 'root'      ,           " +
                        "     'password' = 'root'      ,           " +
                        "     'database-name' = 'rtmk' ,           " +
                        "     'table-name' = 'oms_order'           " +
                        ")"
        );


        // 2. 建表映射 业务库中的 oms_order_id表
        tenv.executeSql(
                "CREATE TABLE item_mysql (                 " +
                        "     id       INT,                   " +
                        "     oid      INT,                   " +
                        "     pid      INT,                   " +
                        "     real_amount    DECIMAL(10,2),   " +
                        "     brand    STRING,                " +
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


        // 3. 关联
        tenv.executeSql(

                " CREATE TEMPORARY VIEW joined AS                                                                    "+
                        " WITH o AS (                                                                                        "+
                        " SELECT                                                                                             "+
                        " *                                                                                                  "+
                        " FROM order_mysql                                                                                   "+
                        " WHERE status in (1,2,3) AND DATE_FORMAT(payment_time,'yyyy-MM-dd') = CURRENT_DATE                  "+
                        " )                                                                                                  "+
                        "                                                                                                    "+
                        " SELECT                                                                                             "+
                        " o.*,                                                                                               "+
                        " i.*,                                                                                               "+
                        " proctime() as pt                                                                                   "+
                        " FROM o                                                                                             "+
                        " JOIN item_mysql i                                                                                  "+
                        " ON o.id = i.oid                                                                                    "
                );

        /* DataStream<Row> joinedStream = tenv.toChangelogStream(tenv.from("joined"));
        // 将changelog流 ，转成了只有+I语义的 append-only流
        SingleOutputStreamOperator<Row> filteredRows = joinedStream.filter(row -> {
            boolean b = "+I".equals(row.getKind().shortString()) || "+U".equals(row.getKind().shortString());
            row.setKind(RowKind.INSERT);
            return b;
        });
        //
        tenv.createTemporaryView("tmp", filteredRows); // 只支持append-only(+I) 语义
        tenv.executeSql("select * from tmp").print(); */


        DataStream<Row> joinedStream = tenv.toChangelogStream(tenv.from("joined"));
        SingleOutputStreamOperator<Row> filteredRows = joinedStream.filter(row -> {
            return "+I".equals(row.getKind().shortString()) || "+U".equals(row.getKind().shortString());
        });
        // 将保留了  +I  +U 变化语义行 的 数据流，重新转回表
        // tenv.createTemporaryView("tmp", filteredRows);  // 只支持append-only(+I) 语义
        tenv.createTemporaryView("filtered",tenv.fromChangelogStream(filteredRows));


        // 4. 窗口统计
        tenv.executeSql(
                " WITH tmp AS (                                                                                             "+
                " SELECT                                                                                                       "+
                        " *,                                                                                                   "+
                        " proctime() as proc_time                                                                              "+
                        " FROM  filtered                                                                                       "+
                        " )                                                                                                    "+
                        "                                                                                                      "+
                        " SELECT                                                                                               "+
                        " *                                                                                                    "+
                        " FROM                                                                                                 "+
                        " (                                                                                                    "+
                        "     SELECT                                                                                           "+
                        "       window_start,                                                                                  "+
                        "       window_end,                                                                                    "+
                        "       brand,                                                                                         "+
                        "       pid,                                                                                           "+
                        "       real_amount,                                                                                   "+
                        "       row_number() over(partition by window_start,window_end,brand order by real_amount desc) as rn  "+
                        "     FROM (                                                                                           "+
                        "     SELECT                                                                                           "+
                        "     	TUMBLE_START(proc_time,INTERVAL '1' MINUTE) as window_start,                                   "+
                        "     	TUMBLE_END(proc_time,INTERVAL '1' MINUTE) as window_end,                                       "+
                        "     	brand,                                                                                         "+
                        "     	pid,                                                                                           "+
                        "     	sum(real_amount) as real_amount                                                                "+
                        "     FROM tmp                                                                                         "+
                        "     GROUP BY                                                                                         "+
                        "         TUMBLE(proc_time,INTERVAL '1' MINUTE),                                                       "+
                        "     	brand,                                                                                         "+
                        "     	pid                                                                                            "+
                        "     ) o1                                                                                             "+
                        " ) o2                                                                                                 "+
                        " WHERE rn<=1	                                                                                       "
                ).print();


        env.execute();
    }
}

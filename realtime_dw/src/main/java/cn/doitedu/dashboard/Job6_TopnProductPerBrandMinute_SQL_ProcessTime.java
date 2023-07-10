package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
public class Job6_TopnProductPerBrandMinute_SQL_ProcessTime {
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
                        " WITH o AS (                                                                                   "+
                        " SELECT                                                                                        "+
                        " *                                                                                             "+
                        " FROM order_mysql                                                                              "+
                        " WHERE status in (1,2,3) AND DATE_FORMAT(payment_time,'yyyy-MM-dd') = CURRENT_DATE             "+
                        " )                                                                                             "+
                        "                                                                                               "+
                        " SELECT                                                                                        "+
                        " o.*,                                                                                          "+
                        " i.*,                                                                                          "+
                        " proctime() as pt                                                                              "+
                        " FROM o                                                                                        "+
                        " JOIN item_mysql i                                                                             "+
                        " ON o.id = i.oid                                                                               "
                );

        // ------------------------------------------------------------------------------------
        // 后续的窗口聚合中，会存在一个问题
        //   假如 在窗口1 [15分 ~ 16分] 中，有如下数据进入系统
        //       +I pid-01, 5000
        //       +I pid-01, 1000
        //   那么，在窗口1触发时，输出的  商品总金额为：  pid-01, 6000
        //   假如 在窗口2 [16分 ~ 17分] 中，有如下数据进入系统
        //       -U pid-01, 5000  (这里是对此前进入的数据的更新）
        //       +U pid-01, 8000  (这里是对此前进入的数据的更新）
        //       pid-01, 1000   (这里是新进入的数据）
        //   那么，在窗口2触发时，咱们期待的结果是什么呢？
        //      在不做任何特别处理时，后续的groupBy(滚动窗口)  将输出：  -5000 + 8000 + 1000 = 4000
        //   如果我们想要的结果是 :  8000 + 1000 = 9000
        //      需要做特别处理：把join后的这个changelog流中的 -U 行过滤掉
        //   具体做法有如下两种：

        /* * 方法1： 通过流式api的辅助，保留 +I 和 +U 语义行，并将 +U 转变成  +I
        // 等价于changelog流 => 纯正的 append-only流
        // 并最终转回视图
        DataStream<Row> joinedStream = tenv.toChangelogStream(tenv.from("joined"));
        // 将 changelog流 ，转成只有+I语义的 append-only流
        SingleOutputStreamOperator<Row> filteredRows = joinedStream.filter(row -> {
            boolean b = "+I".equals(row.getKind().shortString()) || "+U".equals(row.getKind().shortString());
            row.setKind(RowKind.INSERT);
            return b;
        });
        // 将append-only流， 转成 临时视图
        tenv.createTemporaryView("tmp", filteredRows); // 只支持 append-only(+I) 语义
        tenv.executeSql("select * from tmp").print();
        */


        // 方法2： 通过流式api的辅助，保留 +I 和 +U 两种语义行，并最终转回视图
        DataStream<Row> joinedStream = tenv.toChangelogStream(tenv.from("joined"));
        SingleOutputStreamOperator<Row> filteredRows = joinedStream.filter(row -> {
            return "+I".equals(row.getKind().shortString()) || "+U".equals(row.getKind().shortString());
        });
        // 将保留了  +I  +U 变化语义行 的 数据流，重新转回表
        // tenv.createTemporaryView("tmp", filteredRows);  // 只支持 append-only(+I) 语义
        tenv.createTemporaryView("filtered",tenv.fromChangelogStream(filteredRows));

        // ---------------------------------------------------------------------------------

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

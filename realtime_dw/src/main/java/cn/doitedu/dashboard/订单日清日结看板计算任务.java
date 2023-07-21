package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class 订单日清日结看板计算任务 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        TableConfig config = tenv.getConfig();
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","60000 ms");




        // 建表，映射业务库中的  oms_order
        // order_id,total_amount,pay_amount,status, create_time,delivery_time,receive_time
        tenv.executeSql(
                "CREATE TABLE oms_order (    " +
                        "      id BIGINT," +
                        "      total_amount DECIMAL(10,2),             " +
                        "      pay_amount DECIMAL(10,2),               " +
                        "      status int,                             " +
                        "      create_time timestamp(3),               " +
                        "      payment_time timestamp(3),               " +
                        "      delivery_time timestamp(3),             " +
                        "      receive_time timestamp(3),              " +
                        "      rt as  create_time,                     " +
                        "      watermark for rt as rt - interval '0' second,  " +
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'doitedu'   ,               " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'root'      ,               " +
                        "     'database-name' = 'realtimedw',          " +
                        "     'table-name' = 'oms_order'              " +
                        ")"
        );

        //tenv.executeSql("select * from oms_order").print();

        tenv.executeSql(
                "SELECT\n" +
                        "   count(id) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE) as day_order_count,\n" +
                        "   sum(total_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE)  as day_order_total_amount,\n" +
                        "   sum(pay_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE)  as day_order_topay_amount,\n" +
                        "   sum(pay_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE AND status>=1 AND status<=3)  as day_order_payed_amount,\n" +
                        "   count(id) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE AND status=0) as day_order_topay_count,\n" +
                        "   sum(total_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd')=CURRENT_DATE AND status=0) as day_order_topay_total_amount,\n" +
                        "   count(id) FILTER(WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd')=CURRENT_DATE AND status>=1 AND status<=3) as day_order_payed_count,\n" +
                        "   sum(pay_amount) FILTER(WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd')=CURRENT_DATE AND status>=1 AND status<=3)  as day_order_payed_amount2, \n" +
                        "   count(id) FILTER(WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd')=CURRENT_DATE AND status>=2 AND status<=3) as day_order_delivered_count,\n" +
                        "   sum(pay_amount) FILTER(WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd')=CURRENT_DATE AND status>=2 AND status<=3)  as day_order_delivered_amount, \n" +
                        "   count(id) FILTER(WHERE DATE_FORMAT(receive_time,'yyyy-MM-dd')=CURRENT_DATE AND status=3) as day_order_received_count,\n" +
                        "   sum(pay_amount) FILTER(WHERE DATE_FORMAT(receive_time,'yyyy-MM-dd')=CURRENT_DATE AND status=3)  as day_order_received_amount\n" +
                        "FROM oms_order\n" +
                        "GROUP BY \n" +
                        "    TUMBLE(rt,INTERVAL '24' HOUR)").print();

    }


}

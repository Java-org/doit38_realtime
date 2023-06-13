package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OrderDaySettlementSql {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        TableConfig config = tenv.getConfig();
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","5000 ms");



        tenv.executeSql(
                "CREATE TABLE order_mysql (    " +
                        "      id BIGINT," +
                        "      status INT,                        " +
                        "      total_amount decimal(10,2),        " +
                        "      pay_amount decimal(10,2),          " +
                        "      create_time timestamp(3),          " +
                        "      payment_time timestamp(3),         " +
                        "      delivery_time timestamp(3),        " +
                        "      confirm_time timestamp(3),        " +
                        "      update_time timestamp(3),          " +
                        "      rt as update_time        ,         " +
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

          /*
            订单总数、总额 、应付总额 （当日新订单）
            待支付订单数、订单额  （当日新订单）
            支付订单数、订单额  （当日支付）
            发货订单数、订单额  （当日发货）
            完成订单数、订单额  （当日确认）

            -- 每分钟更新一次结果
         */
        tenv.executeSql(
                "WITH  filtered  AS (\n" +
                        "SELECT\n" +
                        "   id,\n" +
                        "   status,\n" +
                        "   total_amount,\n" +
                        "   pay_amount,\n" +
                        "   create_time,\n" +
                        "   payment_time,\n" +
                        "   delivery_time,\n" +
                        "   confirm_time,\n" +
                        "   update_time,\n" +
                        "   rt\n" +
                        "FROM  order_mysql \n" +
                        "WHERE   \n" +
                        "   (date_format(create_time,'yyyy-MM-dd') = current_date  and (status = 0 or status =1 or status =2 or status = 3))\n" +
                        "   or\n" +
                        "   (date_format(create_time,'yyyy-MM-dd') = current_date  and  status = 0)\n" +
                        "   or \n" +
                        "   (date_format(payment_time,'yyyy-MM-dd') = current_date )   \n" +
                        "   or \n" +
                        "   (date_format(delivery_time,'yyyy-MM-dd') = current_date )   \n" +
                        "   or \n" +
                        "   (date_format(confirm_time,'yyyy-MM-dd') = current_date )   \n" +
                        ")\n" +
                        "\n" +
                        "SELECT\n" +
                        "   COUNT(id) FILTER( WHERE date_format(create_time,'yyyy-MM-dd') = current_date) AS cur_day_order_cnt,\n" +
                        "   SUM(total_amount) FILTER( WHERE date_format(create_time,'yyyy-MM-dd') = current_date) AS cur_day_order_total_amount,\n" +
                        "   SUM(pay_amount) FILTER( WHERE date_format(create_time,'yyyy-MM-dd') = current_date) AS cur_day_order_pay_amount,\n" +
                        "   \n" +
                        "   COUNT(id) FILTER( WHERE date_format(create_time,'yyyy-MM-dd') = current_date AND status = 0) AS cur_day_order_cnt,\n" +
                        "   SUM(total_amount) FILTER( WHERE date_format(create_time,'yyyy-MM-dd') = current_date AND status = 0) AS cur_day_order_total_amount,\n" +
                        "   \n" +
                        "   COUNT(id) FILTER( WHERE date_format(payment_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_pay_cnt,\n" +
                        "   SUM(total_amount) FILTER( WHERE date_format(payment_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_pay_total_amount,\n" +
                        "    \n" +
                        "   COUNT(id) FILTER( WHERE date_format(delivery_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_delivery_cnt,\n" +
                        "   SUM(total_amount) FILTER( WHERE date_format(delivery_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_delivery_total_amount,\n" +
                        "       \n" +
                        "   COUNT(id) FILTER( WHERE date_format(confirm_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_confirm_cnt,\n" +
                        "   SUM(total_amount) FILTER( WHERE date_format(confirm_time,'yyyy-MM-dd') = current_date ) AS cur_day_order_confirm_total_amount\n" +
                        "  \n" +
                        "FROM  filtered\n" +
                        "GROUP BY \n" +
                        "   TUMBLE(rt,INTERVAL '24' HOUR)"
        ).print();



    }
}

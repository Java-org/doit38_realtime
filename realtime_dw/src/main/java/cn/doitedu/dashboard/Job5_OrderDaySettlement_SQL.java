package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/17
 * @Desc: 学大数据，上多易教育
 *   订单日清日结看板计算
 **/
public class Job5_OrderDaySettlement_SQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        TableConfig config = tenv.getConfig();
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","60000 ms");

        // 1. 建 cdc 连接器表，映射业务表oms_order
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
                        " rt AS update_time           ,   "+
                        " watermark for rt as rt - interval '0' second ,   "+
                        "     PRIMARY KEY (id) NOT ENFORCED         " +
                        "     ) WITH (                              " +
                        "     'connector' = 'mysql-cdc',            " +
                        "     'hostname' = 'doitedu'   ,            " +
                        "     'port' = '3306'          ,            " +
                        "     'username' = 'root'      ,            " +
                        "     'password' = 'root'      ,            " +
                        "     'database-name' = 'rtmk' ,            " +
                        "     'table-name' = 'oms_order'            " +
                        ")"
        );

        // 2. sql统计
        tenv.executeSql(
                " CREATE TEMPORARY VIEW res AS  SELECT                                                                                             " +
                        "   TUMBLE_START(rt, INTERVAL '24' HOUR) AS window_start,                                                                                                         "+
                        " 	TUMBLE_END(rt, INTERVAL '24' HOUR) AS window_end,                                                                                                               "+
                        " 	PROCTIME() AS  update_time ,                                                                                                               "+
                        " 	COUNT(id) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') =  CURRENT_DATE) AS today_order_total_count,                                                      "+
                        " 	SUM(total_amount) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') =  CURRENT_DATE) AS today_order_total_amount,                                             "+
                        " 	SUM(pay_amount) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') =  CURRENT_DATE) AS today_order_pay_amount,                                                 "+
                        "                                                                                                                                                                   "+
                        " 	COUNT(id) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') =  CURRENT_DATE AND status = 0) AS today_order_topay_count,                                       "+
                        " 	SUM(total_amount) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') =  CURRENT_DATE AND status = 0) AS today_order_topay_amount,                              "+
                        "                                                                                                                                                                   "+
                        " 	COUNT(id) FILTER( WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd') =  CURRENT_DATE AND (status = 1 OR status = 2 or status = 3) ) AS today_payed_count,             "+
                        " 	SUM(total_amount) FILTER( WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd') =  CURRENT_DATE AND (status = 1 OR status = 2 or status = 3) ) AS today_payed_amount,    "+
                        "                                                                                                                                                                   "+
                        " 	COUNT(id) FILTER( WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd') =  CURRENT_DATE AND (status = 2 or status = 3) ) AS today_delivered_count,                      "+
                        " 	SUM(total_amount) FILTER( WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd') =  CURRENT_DATE AND (status = 2 or status = 3) ) AS today_delivered_amount,             "+
                        "                                                                                                                                                                   "+
                        " 	COUNT(id) FILTER( WHERE DATE_FORMAT(confirm_time,'yyyy-MM-dd') =  CURRENT_DATE AND status = 3 ) AS today_confirmed_count,                                       "+
                        " 	SUM(total_amount) FILTER( WHERE DATE_FORMAT(confirm_time,'yyyy-MM-dd') =  CURRENT_DATE AND status = 3 ) AS today_confirmed_amount                               "+
                        "                                                                                                                                                                   "+
                        " FROM order_mysql                                                                                                                                                  "+
                        " GROUP BY                                                                                                                                                          "+
                        "     TUMBLE(rt,INTERVAL '24' HOUR)                                                                                                                                 "
        );


        // 3. 将结果写入mysql
        tenv.executeSql(
                " CREATE TABLE sink_mysql  (             "
                        +" window_start  timestamp NULL            "
                        +" ,window_end  timestamp NULL             "
                        +" ,update_time  timestamp NULL            "
                        +" ,today_order_total_count  bigint        "
                        +" ,today_order_total_amount  decimal(10,0)"
                        +" ,today_order_pay_amount  decimal(10,0)  "
                        +" ,today_order_topay_count  bigint        "
                        +" ,today_order_topay_amount  decimal(10,0)"
                        +" ,today_payed_count  bigint              "
                        +" ,today_payed_amount  decimal(10,0)      "
                        +" ,today_delivered_count  bigint          "
                        +" ,today_devlivered_amount  decimal(10,0) "
                        +" ,today_confirmed_count  bigint          "
                        +" ,today_confirmed_amount  decimal(10,0)  "
                        +" ,PRIMARY KEY(window_start,window_end,update_time) NOT ENFORCED "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/rtmk',      "
                        +"    'table-name' = 'dash_order_day_settlement',    "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );

        tenv.executeSql("insert into sink_mysql select *  from  res");

    }
}

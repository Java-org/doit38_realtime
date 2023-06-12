package cn.doitedu.dashboard;

import cn.doitedu.beans.OrderCdcInnerBean;
import cn.doitedu.beans.OrderCdcOuterBean;
import cn.doitedu.beans.OrderDiffValues;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/8
 * @Desc: 学大数据，上多易教育
 * <p>
 * 订单日清日结看板指标
 **/
@Slf4j
public class Job5_OrderDaySettlement_Sql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        TableConfig config = tenv.getConfig();
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","5000 ms");

        // 1.创建一个cdc连接器的source算子，用于读取 mysql中订单表 binlog

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

        tenv.executeSql(
                " CREATE TEMPORARY VIEW filtered AS                           "+
                        " SELECT                                                      "+
                        "     id,                                                     "+
                        "     status,                                                 "+
                        "     total_amount,                                           "+
                        "     pay_amount,                                             "+
                        "     create_time,                                            "+
                        "     payment_time,                                           "+
                        "     delivery_time,                                          "+
                        "     confirm_time,                                           "+
                        "     update_time,                                            "+
                        "     rt                                                      "+
                        " FROM order_mysql                                            "+
                        " WHERE                                                       "+
                        "     DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE    "+
                        "       OR                                                    "+
                        "     DATE_FORMAT(payment_time,'yyyy-MM-dd') = CURRENT_DATE   "+
                        "       OR                                                    "+
                        "     DATE_FORMAT(delivery_time,'yyyy-MM-dd') = CURRENT_DATE  "+
                        "       OR                                                    "+
                        "     DATE_FORMAT(confirm_time,'yyyy-MM-dd') = CURRENT_DATE   "
        );

         /*
            订单总数、总额 、应付总额 （当日新订单）
            待支付订单数、订单额  （当日新订单）
            支付订单数、订单额  （当日支付）
            发货订单数、订单额  （当日发货）
            完成订单数、订单额  （当日确认）
         */
        tenv.executeSql(
                " SELECT                                                                                                                                 "+
                        "     MAX(rt) AS rt,                                                                                                                     "+
                        "     count(id) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE ) as new_order_cnt,                                   "+
                        "     sum(total_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE) as new_order_total_amount,                    "+
                        "     sum(pay_amount) FILTER(WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE) as new_order_pay_amount,                        "+
                        "                                                                                                                                        "+
                        "     count(id) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 0 ) as to_pay_cnt,                       "+
                        "     sum(total_amount) FILTER( WHERE DATE_FORMAT(create_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 0 ) as to_pay_total_amount,      "+
                        "                                                                                                                                        "+
                        "     count(id) FILTER( WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 1 ) as pay_cnt,                      "+
                        "     sum(total_amount) FILTER( WHERE DATE_FORMAT(payment_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 1 ) as pay_total_amount,     "+
                        "                                                                                                                                        "+
                        "     count(id) FILTER( WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 2 ) as delivery_cnt,                   "+
                        "     sum(total_amount) FILTER( WHERE DATE_FORMAT(delivery_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 2 ) as delivery_total_amount,  "+
                        "                                                                                                                                        "+
                        "     count(id) FILTER( WHERE DATE_FORMAT(confirm_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 3 ) as confirm_cnt,                     "+
                        "     sum(total_amount) FILTER( WHERE DATE_FORMAT(confirm_time,'yyyy-MM-dd') = CURRENT_DATE AND status = 3 ) as confirm_total_amount     "+
                        " FROM  filtered                                                                                                                         "+
                        " GROUP BY                                                                                                                               "+
                        "     TUMBLE(rt , INTERVAL '24' HOUR)                                                                                                    "
        ).print();


    }

}

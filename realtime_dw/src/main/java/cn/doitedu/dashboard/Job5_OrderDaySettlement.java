package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/8
 * @Desc: 学大数据，上多易教育
 *
 * 订单日清日结看板指标
 *
 **/
public class Job5_OrderDaySettlement {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/rtdw/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1.创建一个cdc连接器的逻辑映射表，用于读取 mysql中订单表binlog
        tenv.executeSql(
                "CREATE TABLE order_cdc (    " +
                        " id bigint                     "+
                        " ,member_id bigint             "+
                        " ,create_time timestamp(3)     "+
                        " ,total_amount decimal(10,2)   "+
                        " ,pay_amount decimal(10,2)     "+
                        " ,status int                   "+
                        " ,confirm_status int           "+
                        " ,payment_time timestamp(3)        "+
                        " ,delivery_time timestamp(3)       "+
                        " ,receive_time timestamp(3)        "+
                        " ,modify_time timestamp(3)         "+
                        " ,PRIMARY KEY (id) NOT ENFORCED            " +
                        "  ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',             " +
                        "     'hostname' = 'doitedu'   ,             " +
                        "     'port' = '3306'          ,             " +
                        "     'username' = 'root'      ,             " +
                        "     'password' = 'root'      ,             " +
                        "     'database-name' = 'realtimedw',        " +
                        "     'table-name' = 'oms_order'             " +
                        ")"
        );

        tenv.executeSql("select * from order_cdc").print();






    }
}

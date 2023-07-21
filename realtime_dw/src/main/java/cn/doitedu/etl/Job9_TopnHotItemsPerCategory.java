package cn.doitedu.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/7/20
 * @Desc: 学大数据，上多易教育
 *   最近1小时中，各品类最热门的前10个商品
 *   每分钟更新一次
 *
 **/
public class Job9_TopnHotItemsPerCategory {
    public static void main(String[] args) {

        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);



        // 建表，映射数据源
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                 "
                        + "     user_id           BIGINT,                     "
                        + "     event_id          string,                     "
                        + "     event_time        bigint,                     "
                        + "     properties        map<string,string>,         "
                        + "     proc_time AS proctime()  ,                    "
                        + "     row_time AS to_timestamp_ltz(event_time,3),   "
                        + "     watermark for row_time as row_time - interval '0' second   "
                        + " ) WITH (                                          "
                        + "  'connector' = 'kafka',                           "
                        + "  'topic' = 'dwd_events',                          "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092', "
                        + "  'properties.group.id' = 'testGroup',             "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'value.format'='json',                           "
                        + "  'value.json.fail-on-missing-field'='false',      "
                        + "  'value.fields-include' = 'EXCEPT_KEY')           "
        );


        // 建表，映射商品信息维表
        tenv.executeSql(
                " create table dim_product(                        " +
                        "    id STRING,                               " + //   反转(Long.MAX - 商品id)
                        "    f  ROW<brand_id BIGINT,product_category_id BIGINT>      " +
                        " ) WITH(                                     " +
                        "     'connector' = 'hbase-2.2',              " +
                        "     'table-name' = 'dim_product',           " +
                        "     'zookeeper.quorum' = 'doitedu:2181'     " +
                        " )                                           "
        );


        // 建表，映射mysql中的目标表
        tenv.executeSql(
                " CREATE TABLE res_sink_mysql (                      "
                        +"   window_start timestamp(3),                      "
                        +"   window_end timestamp(3),                        "
                        +"   product_category_id   BIGINT,                   "
                        +"   product_id   BIGINT,                            "
                        +"   show_count   BIGINT,                            "
                        +"   row_number    BIGINT                            "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/doit38',    "
                        +"    'table-name' = 'dashboard_traffic_3',          "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );



        // 计算逻辑
        tenv.executeSql("CREATE TEMPORARY VIEW res AS \n" +
                "WITH tmp AS (\n" +
                "SELECT\n" +
                "  event_id,\n" +
                "  properties['product_id'] as product_id,\n" +
                "  proc_time,\n" +
                "  row_time\n" +
                "from dwd_kafka\n" +
                "where event_id = 'page_load' and properties['product_id'] is not null\n" +
                ")\n" +
                "\n" +
                ",tmp2 AS (\n" +
                "SELECT\n" +
                "   e.product_id,\n" +
                "   p.product_category_id,\n" +
                "   row_time\n" +
                "FROM tmp e \n" +
                "LEFT JOIN dim_product FOR_SYSTEM_TIME AS OF e.proc_time AS p\n" +
                "ON reverse(cast(10000000000-e.product_id as string))  = p.id\n" +
                ")\n" +
                ",tmp3 AS (\n" +
                "SELECT\n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   product_category_id,\n" +
                "   product_id,\n" +
                "   count(1) as show_count\n" +
                "FROM TABLE(\n" +
                "  HOP(TABLE tmp2, DESCRIPTOR(row_time),INTERVAL '1' MINUTE,INTERVAL '1' HOUR)\n" +
                ")\n" +
                "GROUP BY \n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   product_category_id,\n" +
                "   product_id \n" +
                ")\n" +
                "\n" +
                ",tmp4 AS (\n" +
                "SELECT\n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   product_category_id,\n" +
                "   product_id, \n" +
                "   show_count,\n" +
                "   row_number() over(PARTITION BY window_start,window_end,product_category_id ORDER BY show_count DESC) as rn\n" +
                "FROM tmp3 \n" +
                ")\n" +
                "\n" +
                "SELECT\n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   product_category_id,\n" +
                "   product_id, \n" +
                "   show_count,\n" +
                "   rn as row_number" +
                "from  tmp4\n" +
                "where rn<=10");


        // 结果插入mysql
        tenv.executeSql("insert into res_sink_mysql select * from res");

    }


}

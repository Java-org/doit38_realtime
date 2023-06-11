package cn.doitedu.etl;

import cn.doitedu.beans.SearchAggBean;
import cn.doitedu.beans.SearchResultBean;
import cn.doitedu.functions.SimilarWordProcessFunction;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/7
 * @Desc: 学大数据，上多易教育
 *
 *  搜索事件分析主题olap聚合支撑任务
 **/
public class Job5_SearchOlapAggregate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(4);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 读行为明细
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                 "
                        + "     user_id           BIGINT,                     "
                        + "     username          string,                     "
                        + "     session_id        string,                     "
                        + "     event_id          string,                     "
                        + "     event_time        bigint,                     "
                        + "     lat               double,                     "
                        + "     lng               double,                     "
                        + "     release_channel   string,                     "
                        + "     device_type       string,                     "
                        + "     properties        map<string,string>,         "
                        + "     register_phone    STRING,                     "
                        + "     user_status       INT,                        "
                        + "     register_time     TIMESTAMP(3),               "
                        + "     register_gender   INT,                        "
                        + "     register_birthday DATE,                       "
                        + "     register_province STRING,                     "
                        + "     register_city        STRING,                  "
                        + "     register_job         STRING,                  "
                        + "     register_source_type INT,                     "
                        + "     gps_province STRING,                          "
                        + "     gps_city     STRING,                          "
                        + "     gps_region   STRING,                          "
                        + "     page_type    STRING,                          "
                        + "     page_service STRING,                          "
                        + "     proc_time AS proctime(),                      "
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


        // 2.过滤出搜索相关事件
        tenv.executeSql(
                " CREATE TEMPORARY VIEW  search_events  AS SELECT                      "+
                        "   user_id,                                                     "+
                        "   event_id,                                                    "+
                        "   event_time,                                                  "+
                        "   properties['keyword'] as keyword,                            "+
                        "   properties['search_id'] as search_id,                        "+
                        "   cast(properties['res_cnt'] as bigint)  as res_cnt,            "+
                        "   properties['item_id']  as item_id,                            "+
                        "   row_time                                                     "+
                        "                                                                "+
                        " FROM dwd_kafka                                                 "+
                        " WHERE event_id in ('search','search_return','search_click')    "
        );


        // 3. 聚合（一个搜索生命周期中的事件聚合成一条）
        /*  数据规律：
            u1,search,   s_01,苦咖啡,t1, null, null
            u1,search_rt,s_01,苦咖啡,t2, 200 , null
            u1,search_ck,s_01,苦咖啡,t3, null, item01

             结果： user_id,search_id,搜索词,start_time,返回条数，点击总次数
             逻辑： 在时间窗口内，进行分组聚合即可：
                      group by:  user_id,search_id,keyword
                   * startTime:  min(event_time)
                   * 返回结果数:  max(返回结果数)
                   * 点击次数  ： count(if(点击事件))
         */
        tenv.executeSql(
                " CREATE TEMPORARY VIEW agg AS                                                "+
                        " SELECT                                                                 "+
                        " 	user_id,                                                             "+
                        " 	keyword,                                                             "+
                        " 	search_id,                                                           "+
                        " 	min(event_time) as search_time,                                      "+
                        " 	max(res_cnt) as return_item_count,                                   "+
                        " 	COALESCE(sum(if(event_id='search_click',1,0)),0) as click_item_count "+
                        " FROM TABLE(                                                            "+
                        "  TUMBLE(TABLE search_events,DESCRIPTOR(row_time),INTERVAL '5' MINUTE)  "+
                        " )                                                                      "+
                        " GROUP BY                                                               "+
                        "   window_start,                                                        "+
                        " 	window_end,                                                          "+
                        " 	user_id,                                                             "+
                        " 	keyword,                                                             "+
                        " 	search_id                                                            "
        );

        // tenv.executeSql("select * from agg").print();
        /*
        +----+----------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------+
        | op |              user_id |                        keyword |                      search_id |           search_time |                        res_cnt |   click_cnt |
        +----+----------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------+
        | +I |                    3 |                   usb 移动固态 |                           sc01 |        1670596213000 |                            276 |           2 |
        | +I |                    5 |                     速溶苦咖啡 |                           sc03 |        1670596215000 |                            186 |           3 |
        | +I |                    3 |                   固态移动硬盘 |                           sc02 |        1670596215000 |                            276 |           0 |
       */

        // 4. 查询近义词和分词(http请求)
        Table aggTable = tenv.from("agg");
        DataStream<SearchAggBean> dataStream = tenv.toDataStream(aggTable, SearchAggBean.class);
        //dataStream.print();


        SingleOutputStreamOperator<SearchResultBean> resultStream = dataStream.keyBy(bean -> Md5Crypt.md5Crypt(bean.getKeyword().getBytes()).substring(0, 5))
                .process(new SimilarWordProcessFunction());


        // 5. 写入doris
        // 流转表
        tenv.createTemporaryView("res",resultStream);
        // 创建doris的连接器表
        tenv.executeSql(
                "CREATE TABLE sink_doris(             "+
                        " user_id           BIGINT,      "+
                        " search_id         STRING,      "+
                        " keyword           STRING,      "+
                        " split_words       STRING,      "+
                        " similar_word      STRING,      "+
                        " search_time       BIGINT,      "+
                        " return_item_count BIGINT,      "+
                        " click_item_count  BIGINT       "+
                        ") WITH (                                        "+
                        "   'connector' = 'doris',                       "+
                        "   'fenodes' = 'doitedu:8030',                  "+
                        "   'table.identifier' = 'dws.search_ana_agg',   "+
                        "   'username' = 'root',                         "+
                        "   'password' = 'root',                         "+
                        "   'sink.label-prefix' = 'doris_label35'        "+
                        ")                                               "
        );

        tenv.executeSql("INSERT INTO sink_doris  " +
                "SELECT " +
                "user_id,search_id,keyword,split_words,similar_word,search_time,return_item_count,click_item_count " +
                "FROM res");

        env.execute();
    }
}

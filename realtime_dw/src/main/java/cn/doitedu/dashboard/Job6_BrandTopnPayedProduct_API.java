package cn.doitedu.dashboard;

import cn.doitedu.beans.ItemCdcOuterBean;
import cn.doitedu.beans.OrderCdcOuterBean;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/10
 * @Desc: 学大数据，上多易教育
 *
 *   实时看板指标： 每小时 ，每个品牌中， 已支付金额最大的前 N个商品
 **/
public class Job6_BrandTopnPayedProduct_API {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource1 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> orderCdcStream = env.fromSource(mySqlSource1, WatermarkStrategy.noWatermarks(), "cdc");
        SingleOutputStreamOperator<OrderCdcOuterBean> orderOuterBeanStream = orderCdcStream.map(json -> JSON.parseObject(json, OrderCdcOuterBean.class));


        MySqlSource<String> mySqlSource2 = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order_item") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> itemCdcStream = env.fromSource(mySqlSource1, WatermarkStrategy.noWatermarks(), "cdc");
        SingleOutputStreamOperator<ItemCdcOuterBean> itemOuterBeanStream = itemCdcStream.map(json -> JSON.parseObject(json, ItemCdcOuterBean.class));



      /*  orderOuterBeanStream
                .join(itemOuterBeanStream)
                .where()
                .equalTo()
                .window()
      */

       /*orderOuterBeanStream
                .coGroup(itemOuterBeanStream)
                .where()
                .equalTo()
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new CoGroupFunction<OrderCdcOuterBean, ItemCdcOuterBean, String>() {
                    @Override
                    public void coGroup(Iterable<OrderCdcOuterBean> iterable, Iterable<ItemCdcOuterBean> iterable1, Collector<String> collector) throws Exception {
                        // 订单时间 与 商品数据时间 不匹配时，将无法关联
                        // 比如   订单未支付，到达；  商品也到达，不计算
                        // 但订单 在下一个窗口时 变成已支付，则拿不到商品数据了
                    }
                });
        */






    }

}

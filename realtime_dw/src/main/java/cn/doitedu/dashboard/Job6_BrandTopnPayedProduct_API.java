package cn.doitedu.dashboard;

import cn.doitedu.beans.*;
import cn.doitedu.functions.TopnKeyedBroadcastProcessFunction;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/10
 * @Desc: 学大数据，上多易教育
 * <p>
 * 实时看板指标： 每小时 ，每个品牌中， 已支付金额最大的前 N个商品
 **/
public class Job6_BrandTopnPayedProduct_API {
    public static void main(String[] args) throws Exception {

        // 'taskmanager.memory.network.fraction',
        // 'taskmanager.memory.network.min',
        // 'taskmanager.memory.network.max'.

        Configuration conf = new Configuration();
        //conf.setDouble("taskmanager.memory.network.fraction",0.2);
        //conf.setString("taskmanager.memory.network.min","128mb");
        //conf.setString("taskmanager.memory.network.max","1gb");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
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
        DataStreamSource<String> itemCdcStream = env.fromSource(mySqlSource2, WatermarkStrategy.noWatermarks(), "cdc");
        SingleOutputStreamOperator<ItemCdcOuterBean> itemOuterBeanStream = itemCdcStream.map(json -> JSON.parseObject(json, ItemCdcOuterBean.class));

        // 1. 将订单主表数据广播出去
        MapStateDescriptor<Long, OrderCdcInnerBean> orderStateDesc = new MapStateDescriptor<>("order-state", Long.class, OrderCdcInnerBean.class);

        BroadcastStream<OrderCdcInnerBean> broadcast = orderOuterBeanStream
                .map(OrderCdcOuterBean::getAfter)
                .broadcast(orderStateDesc);


        orderOuterBeanStream.print();
        itemOuterBeanStream.print();

        // 2. 对item数据，按品牌分区 ,连接广播流
        itemOuterBeanStream
                .map(ItemCdcOuterBean::getAfter)
                .keyBy(ItemCdcInnerBean::getProduct_brand)
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, String>() {
                    @Override
                    public void processElement(ItemCdcInnerBean itemCdcInnerBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

                        System.out.println("p----" + itemCdcInnerBean);
                    }

                    @Override
                    public void processBroadcastElement(OrderCdcInnerBean orderCdcInnerBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, String>.Context context, Collector<String> collector) throws Exception {
                        System.out.println("b----" + orderCdcInnerBean);
                    }
                })
                .print();

        env.execute();

    }

}

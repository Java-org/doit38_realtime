package cn.doitedu.dashboard;

import cn.doitedu.beans.OrderCdcInnerBean;
import cn.doitedu.beans.OrderCdcOuterBean;
import cn.doitedu.beans.OrderDiffValues;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverterConfig;

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
public class Job5_OrderDaySettlement {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/rtdw/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1.创建一个cdc连接器的source算子，用于读取 mysql中订单表 binlog

        // 用于指定 JsonDebeziumDeserializationSchema在对decimal类型进行反序列化时的返回格式（数字格式）
        HashMap<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("doitedu")
                .port(3306)
                .databaseList("realtimedw") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("realtimedw.oms_order") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema(false, schemaConfigs)) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc");

        /*
            订单总数、总额 、应付总额 （当日新订单）
            待支付订单数、订单额  （当日新订单）
            支付订单数、订单额  （当日支付）
            发货订单数、订单额  （当日发货）
            完成订单数、订单额  （当日确认）
         */
        SingleOutputStreamOperator<OrderCdcOuterBean> beanStream
                = cdcStream.map(json -> JSON.parseObject(json, OrderCdcOuterBean.class));

        /**
         * 计算整体方案
         *   1. 因为上述指标都是全局聚合值，所有一定需要一个最终的单并行度算子
         *   2. 但也不能整个程序就一个单并行task来处理所有数据和逻辑，我们设计了一个2层的计算方案
         *     2.1  keyBy(order_id) 后，对同一个订单的 changelog 数据按照需求逻辑输出：调整值
         *     2.2  全局单并行：把上游输入过来的持续不断的 “调整值“ 进行累加
         */
        beanStream.keyBy(outer -> outer.getAfter().getId())
                .process(new KeyedProcessFunction<Long, OrderCdcOuterBean, OrderDiffValues>() {
                    @Override
                    public void processElement(OrderCdcOuterBean orderCdcOuterBean, KeyedProcessFunction<Long, OrderCdcOuterBean, OrderDiffValues>.Context context, Collector<OrderDiffValues> collector) throws Exception {
                        OrderCdcInnerBean before = orderCdcOuterBean.getBefore();
                        OrderCdcInnerBean after = orderCdcOuterBean.getAfter();
                        String op = orderCdcOuterBean.getOp();

                        // 先构造一个初始值全为0的调整结果bean
                        OrderDiffValues orderDiffValues = new OrderDiffValues();

                        // 订单总数调整
                        long truncate = 24 * 60 * 60 * 1000L;
                        // 如果before无效，而现在有效，则订单数要 +1
                        if ((before == null || before.getStatus() >= 4) && after.getStatus() <= 3) {
                            orderDiffValues.setTotalCount(1);
                        }
                        // 如果 before有效，而现在无效，则订单数要 -1
                        else if ((before != null || before.getStatus() <= 3) && after.getStatus() >= 4) {
                            orderDiffValues.setTotalCount(-1);
                        }

                        // before无效，现在无效  ^   before 有效 ，现在有效
                        else {
                            orderDiffValues.setTotalCount(0);
                        }

                        // 订单原价总额调整


                        // 订单实付总额调整


                        // 待支付订单数调整

                        // 待支付订单额调整


                        // 支付订单数调整


                        // 支付订单额调整


                        // 发货订单数调整


                        // 发货订单额调整


                        // 完成订单数调整


                        // 完成订单额调整


                    }
                });


        env.execute();

    }
}

package cn.doitedu.etl;

import cn.doitedu.beans.TimelongBean;
import cn.doitedu.functions.TimeLongProcessFunction;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/6
 * @Desc: 学大数据，上多易教育
 * <p>
 * 访问时长olap分析的支撑etl聚合任务
 * 聚合粒度： 每个人每次打开一个页面的  ： 起始时间，结束时间
 **/
public class Job3_AccessTimeAnalysisOlapAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 创建映射表，映射kafka明细层的行为日志
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-g01")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加source，得到源数据流
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // 将源数据流中的json格式数据，转成javaBean数据
        SingleOutputStreamOperator<TimelongBean> beanStream = ds.map(json -> {

            TimelongBean timelongBean = JSON.parseObject(json, TimelongBean.class);
            timelongBean.setPage_url(timelongBean.getProperties().get("url"));
            timelongBean.setProperties(null);

            return timelongBean;
        });


        // 核心逻辑
        SingleOutputStreamOperator<TimelongBean> resultStream = beanStream
                .keyBy(bean -> bean.getSession_id())
                .process(new TimeLongProcessFunction());

        resultStream.print();

        env.execute();


    }
}

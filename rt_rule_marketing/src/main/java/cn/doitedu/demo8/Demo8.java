package cn.doitedu.demo8;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/13
 * @Desc: 学大数据，上多易教育
 * 实时监控app上的所有用户的所有行为
 * 相较 demo6的变化： 规则运算机池 在failover后的 恢复机制
 **/

@Slf4j
public class Demo8 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "D:\\ckpt\\1838ba15b6fff619ec9967750c284eed\\chk-118");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-jyjy")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加source，得到源数据流
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // json解析成javaBean
        SingleOutputStreamOperator<UserEvent> userEventBeanStream = ds.map(json -> JSON.parseObject(json, UserEvent.class));


        /**
         * 用cdc去监听规则的元数据库
         */
        tenv.executeSql(
                "CREATE TABLE rule_meta_mysql (     " +
                        "      rule_id STRING,         " +
                        "      rule_model_id STRING,   " +
                        "      rule_param_json STRING, " +
                        "      online_status INT,      " +
                        "      pre_select_crowd BYTES,  " +
                        "     PRIMARY KEY (rule_id) NOT ENFORCED  " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu'   ,          " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'root'      ,          " +
                        "     'database-name' = 'doit38',         " +
                        "     'table-name' = 'rule_meta'          " +
                        ")"
        );
        DataStream<Row> ruleMetaStream = tenv.toChangelogStream(tenv.from("rule_meta_mysql"));
        SingleOutputStreamOperator<RuleMetaBean> ruleMetaBeanStream = ruleMetaStream.map(new MapFunction<Row, RuleMetaBean>() {
            @Override
            public RuleMetaBean map(Row row) throws Exception {
                String ruleId = row.getFieldAs("rule_id");
                String ruleModelId = row.getFieldAs("rule_model_id");
                String ruleParamJson = row.getFieldAs("rule_param_json");
                int onlineStatus = row.getFieldAs("online_status");

                // demo6新增 : 取出预圈选的人群序列化字节
                byte[] crowdBytes = row.getFieldAs("pre_select_crowd");
                // 反序列化成 RoaringBitmap 对象
                Roaring64Bitmap crowdBitmap = Roaring64Bitmap.bitmapOf();
                if (crowdBytes != null) {
                    crowdBitmap.deserialize(ByteBuffer.wrap(crowdBytes));
                }

                RowKind kind = row.getKind();
                String op = kind.shortString();

                return new RuleMetaBean(op, ruleId, ruleModelId, ruleParamJson, onlineStatus, crowdBitmap);
            }
        });

        // 广播规则定义数据
        MapStateDescriptor<String, RuleMetaBean> desc = new MapStateDescriptor<>("calculator-map", String.class, RuleMetaBean.class);
        BroadcastStream<RuleMetaBean> broadcast = ruleMetaBeanStream.broadcast(desc);


        // 规则是否满足的判断核心逻辑
        SingleOutputStreamOperator<String> messages
                = userEventBeanStream
                .keyBy(UserEvent::getUser_id)
                .connect(broadcast)  // 用户行为数据流  连接  规则元数据广播流
                .process(new KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>() {

                    ConcurrentHashMap<String, RuleModelCalculator> calculatorHashMap = new ConcurrentHashMap<>();


                    @Override
                    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

                        // 取到广播状态
                        ReadOnlyBroadcastState<String, RuleMetaBean> metaBeanBroadcastState = readOnlyContext.getBroadcastState(desc);
                        if (calculatorHashMap.size() == 0) {
                            failoverRestore(metaBeanBroadcastState, calculatorHashMap, getRuntimeContext());
                        }

                        Set<Map.Entry<String, RuleModelCalculator>> entries = calculatorHashMap.entrySet();
                        for (Map.Entry<String, RuleModelCalculator> entry : entries) {
                            // 取出当前遍历到的运算机
                            RuleModelCalculator calculator = entry.getValue();

                            if(userEvent.getEvent_id().equals("p") && RandomUtils.nextInt(0,10) % 6 == 0){
                                throw new RuntimeException("特意预埋的异常......");
                            }

                            // 调用运算机，处理当前收到的用户行为
                            calculator.calculate(userEvent, collector);
                        }

                    }

                    /**
                     *
                     * @param ruleMetaBean
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.Context context, Collector<String> collector) throws Exception {

                        // 取到广播状态
                        BroadcastState<String, RuleMetaBean> metaBeanBroadcastState = context.getBroadcastState(desc);
                        if (calculatorHashMap.size() == 0) {
                            failoverRestore(metaBeanBroadcastState, calculatorHashMap, getRuntimeContext());
                        }

                        // 取出规则元数据中的各个字段
                        String ruleModelId = ruleMetaBean.getRuleModelId();
                        String ruleId = ruleMetaBean.getRuleId();
                        String ruleParamJson = ruleMetaBean.getRuleParamJson();
                        int onlineStatus = ruleMetaBean.getOnlineStatus();
                        String op = ruleMetaBean.getOp();

                        // TODO  按照模型的验证规则，对输入的规则定义信息进行校验（尤其是对参数json要进行合规校验）

                        // demo6 新增： 预圈选人群
                        Roaring64Bitmap preSelectedCrowd = ruleMetaBean.getPreSelectedCrowd();

                        // 如果收到的数据 是 +I ,+U ,且 online_status = 2(上线)
                        if (("+I".equals(op) || "+U".equals(op)) && onlineStatus == 2) {
                            // 根据 本次注入的 新规则，所属的模型，构造该模型的运算机对象
                            RuleModelCalculator calculator = null;
                            if ("model-001".equals(ruleModelId)) {
                                calculator = new RuleModel1ModelCalculator();
                                // 初始化该运算机对象
                                calculator.init(ruleParamJson, getRuntimeContext(), preSelectedCrowd);

                                // 将元数据 bean，放入广播状态
                                metaBeanBroadcastState.put(ruleId,ruleMetaBean);


                            } else if ("model-002".equals(ruleModelId)) {
                                calculator = new RuleModel2ModelCalculator();
                                // 初始化该运算机对象
                                calculator.init(ruleParamJson, getRuntimeContext(), preSelectedCrowd);

                                // 将元数据 bean，放入广播状态
                                metaBeanBroadcastState.put(ruleId,ruleMetaBean);

                            }
                            // 将初始化好的规则的运算机对象，放入广播状态
                            calculatorHashMap.put(ruleId, calculator);

                            log.warn("新增或修改了一个规则:{},所属模型:{}", ruleId, ruleModelId);


                        } else if ("-D".equals(op) || onlineStatus != 2) {
                            calculatorHashMap.remove(ruleId);

                            // 从广播状态中 ，移除失效的 规则元数据
                            metaBeanBroadcastState.remove(ruleId);


                            log.warn("删除或下线了一个规则:{},所属模型:{}", ruleId, ruleModelId);
                        }

                    }
                });

        messages.print();

        env.execute();

    }


    private static void failoverRestore(ReadOnlyBroadcastState<String, RuleMetaBean> metaBeanBroadcastState,
                                        ConcurrentHashMap<String, RuleModelCalculator> calculatorHashMap,
                                        RuntimeContext runtimeContext) throws Exception {
        Iterable<Map.Entry<String, RuleMetaBean>> entries = metaBeanBroadcastState.immutableEntries();

        for (Map.Entry<String, RuleMetaBean> entry : entries) {
            RuleMetaBean metaBean = entry.getValue();
            // 取出规则元数据中的各个字段
            String ruleModelId = metaBean.getRuleModelId();
            String ruleId = metaBean.getRuleId();
            String ruleParamJson = metaBean.getRuleParamJson();
            Roaring64Bitmap preSelectedCrowd = metaBean.getPreSelectedCrowd();

            RuleModelCalculator calculator;
            if ("model-001".equals(ruleModelId)) {
                calculator = new RuleModel1ModelCalculator();
                // 初始化该运算机对象
                calculator.init(ruleParamJson, runtimeContext, preSelectedCrowd);
                calculatorHashMap.put(ruleId, calculator);
                log.warn("恢复了一个运算机,rule_id:{}, model_id:{} ", ruleId, ruleModelId);

            } else if ("model-002".equals(ruleModelId)) {
                calculator = new RuleModel1ModelCalculator();
                // 初始化该运算机对象
                calculator.init(ruleParamJson, runtimeContext, preSelectedCrowd);
                calculatorHashMap.put(ruleId, calculator);
                log.warn("恢复了一个运算机,rule_id:{}, model_id:{} ", ruleId, ruleModelId);
            }
        }
    }


}

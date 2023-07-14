package cn.doitedu.demo10_doit39;

import cn.doitedu.demo10_doit39.beans.RuleMetaBean;
import cn.doitedu.demo10_doit39.beans.UserEvent;
import com.alibaba.fastjson.JSON;
import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/7/11
 * @Desc: 学大数据，上多易教育
 * 相对于demo5：
 * 参数实现动态注入
 * 可以实现固定规则模型下的  规则动态上下线
 **/
@Slf4j
public class Demo10 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doitedu-gyy")
                .setTopics("dwd_events")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加source，得到源数据流
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // json解析成javaBean
        SingleOutputStreamOperator<UserEvent> beanStream = ds.map(json -> JSON.parseObject(json, UserEvent.class));

        // keyby
        KeyedStream<UserEvent, Long> keyedEventStream = beanStream.keyBy(UserEvent::getUser_id);

        // 利用cdc连接器，读取规则元数据表
        tableEnv.executeSql(
                " create table rule_meta (                 " +
                        "    rule_id string,                       " +
                        "    param_json string,                    " +
                        "    calculator_source_code string,        " +
                        "    status  int,                          " +
                        "    target_users  bytes,                  " +
                        "    PRIMARY KEY (rule_id) NOT ENFORCED    " +
                        " ) WITH (                                 " +
                        " 'connector' = 'mysql-cdc'   ,            " +
                        " 'hostname' = 'doitedu'      ,            " +
                        " 'port' = '3306'             ,            " +
                        " 'username' = 'root'         ,            " +
                        " 'password' = 'root'         ,            " +
                        " 'database-name' = 'doit39'  ,            " +
                        " 'table-name' = 'rule_meta'               " +
                        " )                                        "
        );

        Table table = tableEnv.from("rule_meta");
        DataStream<Row> ruleMetaRowStream = tableEnv.toChangelogStream(table);
        SingleOutputStreamOperator<RuleMetaBean> ruleMetaBeanStream = ruleMetaRowStream.map(new MapFunction<Row, RuleMetaBean>() {
            @Override
            public RuleMetaBean map(Row row) throws Exception {

                RowKind kind = row.getKind();
                int operateType = kind.toByteValue();

                String ruleId = row.getFieldAs("rule_id");
                String paramJson = row.getFieldAs("param_json");
                int status = row.getFieldAs("status");
                String calculatorSourceCode = row.getFieldAs("calculator_source_code");

                // 取出数据row中的bitmap
                byte[] bitmapBytes = row.getFieldAs("target_users");

                // 反序列这个字节，成为一个bitmap对象
                Roaring64Bitmap targetUsers = Roaring64Bitmap.bitmapOf();
                targetUsers.deserialize(ByteBuffer.wrap(bitmapBytes));

                return new RuleMetaBean(operateType, ruleId, paramJson, calculatorSourceCode, status, targetUsers);
            }
        });

        // 广播规则元数据流 （广播状态未来会存储：  规则id->元数据bean）
        MapStateDescriptor<String, RuleMetaBean> bc_desc = new MapStateDescriptor<>("rule_meta_bs", String.class, RuleMetaBean.class);
        BroadcastStream<RuleMetaBean> ruleMetaBs = ruleMetaBeanStream.broadcast(bc_desc);


        // 将用户行为数据主流，连接规则元数据广播流
        BroadcastConnectedStream<UserEvent, RuleMetaBean> connected = keyedEventStream.connect(ruleMetaBs);

        // 对连接好的两个流，执行process
        connected.process(
                new KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>() {
                    HashMap<String, RuleCalculator> calculatorHashMap = new HashMap<>();
                    GroovyClassLoader groovyClassLoader;
                    ListState<UserEvent> recentEventState;
                    ValueState<Set<String>> ruleIdSetState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        groovyClassLoader = new GroovyClassLoader();

                        // 申请一个List状态
                        ListStateDescriptor<UserEvent> listStateDescriptor = new ListStateDescriptor<>("recent_events", UserEvent.class);
                        listStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());

                        recentEventState = getRuntimeContext().getListState(listStateDescriptor);

                        // 申请一个状态，用来辨别运算机是否新上线的
                        ruleIdSetState = getRuntimeContext().getState(new ValueStateDescriptor<Set<String>>("ruleIdSet", TypeInformation.of(new TypeHint<Set<String>>() {
                        })));

                    }

                    /**
                     *  负责主流数据处理
                     */
                    @Override
                    public void processElement(UserEvent event, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        Set<String> oldRuleIdSet = ruleIdSetState.value();
                        if(oldRuleIdSet == null) {
                            oldRuleIdSet = new HashSet<>();
                            ruleIdSetState.update(oldRuleIdSet);
                        }

                        ReadOnlyBroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(bc_desc);

                        // 如果运算机池是空的，则说明： 要么系统是新启动，要么是运行过程中出现故障并重启了
                        if (calculatorHashMap.isEmpty()) {
                            // 就需要对运算机池进行恢复
                            for (Map.Entry<String, RuleMetaBean> immutableEntry : broadcastState.immutableEntries()) {
                                String ruleId = immutableEntry.getKey();
                                RuleMetaBean ruleMetaBean = immutableEntry.getValue();
                                // 根据这条规则元数据bean，恢复出一个运算机
                                RuleCalculator ruleCalculator = generateRuleCalculator(ruleMetaBean, groovyClassLoader, getRuntimeContext());
                                log.warn("在processElement中,恢复了一个规则运算机,ruleId:{}",ruleId);
                                // 把运算机放入运算机池
                                calculatorHashMap.put(ruleId,ruleCalculator);
                            }
                        }

                        for (Map.Entry<String, RuleCalculator> entry : calculatorHashMap.entrySet()) {
                            String ruleId = entry.getKey();
                            RuleCalculator ruleCalculator = entry.getValue();

                            // 判断当前遍历到的运算机，是否是一个"新上线的运算机"且需要处理 recent 数据
                            if( !ruleIdSetState.value().contains(ruleId)){
                                // 将 recent 数据，逐个喂给该运算机去处理
                                for (UserEvent userEvent : recentEventState.get()) {
                                    ruleCalculator.calculate(userEvent,out);
                                }

                                // 将本新运算机，加入 老规则集合
                                ruleIdSetState.value().add(ruleId);

                            }

                            // 处理本次收到的event
                            ruleCalculator.calculate(event, out);
                        }

                        // 将本次收到的行为数据，缓存到10秒状态中
                        recentEventState.add(event);

                    }

                    /**
                     * 负责广播流数据处理
                     */
                    @Override
                    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMetaBean, String>.Context ctx, Collector<String> out) throws Exception {

                        BroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(bc_desc);

                        if (calculatorHashMap.isEmpty()) {
                            // 就需要对运算机池进行恢复
                            for (Map.Entry<String, RuleMetaBean> immutableEntry : broadcastState.immutableEntries()) {
                                String ruleId = immutableEntry.getKey();
                                RuleMetaBean metaBean = immutableEntry.getValue();
                                // 根据这条规则元数据bean，恢复出一个运算机
                                RuleCalculator ruleCalculator = generateRuleCalculator(metaBean, groovyClassLoader, getRuntimeContext());
                                log.warn("在processBroadcastElement中,恢复了一个规则运算机,ruleId:{}",ruleId);
                                // 把运算机放入运算机池
                                calculatorHashMap.put(ruleId,ruleCalculator);
                            }
                        }

                        // 0-> +I  1-> -U  2-> +U 3-> -D
                        int operateType = ruleMetaBean.getOperateType();
                        // 表示规则管理系统的规则元数据表中，新增了一条规则的信息
                        if (operateType == 0 || operateType == 2) {

                            log.warn("收到一条新增规则的cdc数据, 规则id为:{}", ruleMetaBean.getRuleId());
                            // 取出元数据中的规则参数json
                            RuleCalculator ruleCalculator = generateRuleCalculator(ruleMetaBean, groovyClassLoader, getRuntimeContext());

                            // 将运算机放入运算机池
                            calculatorHashMap.put(ruleMetaBean.getRuleId(), ruleCalculator);

                            // 将元数据bean，放入广播状态
                            broadcastState.put(ruleMetaBean.getRuleId(), ruleMetaBean);

                            log.warn("规则上线完成,规则id:{},此刻，运算机池的size：{}", ruleMetaBean.getRuleId(), calculatorHashMap.size());
                        }

                        if (operateType == 3) {
                            log.warn("收到一条下线则的cdc数据, 规则id为:{}", ruleMetaBean.getRuleId());

                            // 下线规则
                            calculatorHashMap.remove(ruleMetaBean.getRuleId());

                            // 从广播状态中，移除这个规则的ruleMetaBean
                            broadcastState.remove(ruleMetaBean.getRuleId());


                            log.warn("规则下线完成,规则id:{},此刻，运算机池的size：{}", ruleMetaBean.getRuleId(), calculatorHashMap.size());
                        }
                    }

                    private RuleCalculator generateRuleCalculator(RuleMetaBean ruleMetaBean, GroovyClassLoader groovyClassLoader, RuntimeContext runtimeContext) throws InstantiationException, IllegalAccessException, IOException {
                        String paramJson = ruleMetaBean.getParamJson();

                        // 构造运算机
                        // RuleModel_1_Calculator ruleCalculator = new RuleModel_1_Calculator();

                        // demo9迭代升级： 根据注入的规则元信息中的运算机源代码来构造运算机实例对象
                        String calculatorSourceCode = ruleMetaBean.getCalculatorSourceCode();
                        // 编译，加载
                        Class aClass = groovyClassLoader.parseClass(calculatorSourceCode);

                        // 反射，实例化
                        RuleCalculator ruleCalculator = (RuleCalculator) aClass.newInstance();

                        // 初始化运算机
                        ruleCalculator.init(paramJson, runtimeContext, ruleMetaBean.getTargetUsers());
                        return ruleCalculator;
                    }


                }).print();

        env.execute();
    }

    /**
     * 工具方法：根据传入的规则元数据RuleMetaBean，生成并初始化一个运算机
     */


}

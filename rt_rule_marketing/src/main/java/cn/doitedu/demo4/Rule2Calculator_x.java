package cn.doitedu.demo4;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * 规则 2 :
 * 静态画像条件： active_level=2  AND gender=male
 * 动态画像条件： 规则上线后，依次发生过：[x, y ,z,p]行为序列 ,3次以上
 * 触发条件： 发生 m 行为(  eventId=c,properties[p1]=v1 )
 */
public class Rule2Calculator_x implements RuleCalculator{

    ValueState<Integer> indexState;
    Table table;
    JSONObject message;

    String[] eventSeqCondition = {"k","b","c"};

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {
        indexState = runtimeContext.getState(new ValueStateDescriptor<Integer>("index_state", Integer.class));

        // 创建hbase连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","doitedu:2181");

        Connection conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("user_profile"));

        // 构造一个用于输出命中消息的对象
        message = new JSONObject();
        message.put("rule_id", "rule-002");


    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        /* *
         * 动态画像条件统计
         */
        Integer index = indexState.value();
        index = (index == null? 0 : index);
        if(index < 3 && userEvent.getEvent_id().equals(eventSeqCondition[index])){
            indexState.update(index+1);
        }

        /**
         * 触发后的条件满足与否判断
         */
        if(userEvent.getEvent_id().equals("c") && userEvent.getProperties().getOrDefault("p1","").equals("v1") ){

            // 进而判断动态画像条件是否满足
            if (indexState.value() >= 3) {

                // 进而判断用户的静态画像条件是否满足
                Get get = new Get(Bytes.toBytes(userEvent.getUser_id()));
                get.addColumn("f".getBytes(), "active_level".getBytes());
                get.addColumn("f".getBytes(), "gender".getBytes());

                Result result = table.get(get);
                byte[] activeBytes = result.getValue("f".getBytes(), "active_level".getBytes());
                String activeLevelStr = Bytes.toString(activeBytes);
                int activeLevel = Integer.parseInt(activeLevelStr);

                byte[] genderBytes = result.getValue("f".getBytes(), "gender".getBytes());
                String gender = Bytes.toString(genderBytes);

                if (activeLevel == 3 && "male".equals(gender)) {
                    // 如果静态画像条件也满足，则输出规则命中消息
                    message.put("user_id", userEvent.getUser_id());
                    message.put("match_time", userEvent.getEvent_time());

                    collector.collect(message.toJSONString());
                }
            }
        }

    }
}

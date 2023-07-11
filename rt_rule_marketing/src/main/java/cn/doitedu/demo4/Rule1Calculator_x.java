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
 * 规则 1 :
 * 静态画像条件： 画像标签 age=30 AND gender=male
 * 实时动态画像标签条件：在规则上线后，该用户发生 3次 add_cart 行为
 * 规则触发条件： 发生x行为
 *
 *
 * rule-2
 * 静态画像条件： 画像标签 job=程序员 AND age:[20,30] and 月活跃天数 > 20
 * 实时动态画像标签条件：在规则上线后，该用户发生 2次 点赞 行为
 * 规则触发条件： 发生 分享 行为 且分享的方式为 qq空间
 *
 *
 */
public class Rule1Calculator_x implements RuleCalculator{
    ValueState<Integer> wCntState;
    Table table;
    JSONObject message;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {
        wCntState = runtimeContext.getState(new ValueStateDescriptor<Integer>("cnt_state", Integer.class));

        // 创建hbase连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","doitedu:2181");

        Connection conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("user_profile"));

        // 构造一个用于输出命中消息的对象
        message = new JSONObject();
        message.put("rule_id", "rule-001");

    }


    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        long userId = userEvent.getUser_id();

        // 动态画像条件统计
        if (userEvent.getEvent_id().equals("w")) {
            wCntState.update(wCntState.value() == null ? 1 : wCntState.value() + 1);
        }

        // 规则被触发后的判断
        if (userEvent.getEvent_id().equals("x")) {

            // 先判断动态统计画像条件是否已满足
            if (wCntState.value() != null && wCntState.value() >= 3) {

                // 如果动态画像条件以满足，进而判断该受众是否满足规则中的静态画像条件
                Get get = new Get(Bytes.toBytes(userId));
                get.addColumn("f".getBytes(), "age".getBytes());
                get.addColumn("f".getBytes(), "gender".getBytes());

                Result result = table.get(get);
                byte[] ageBytes = result.getValue("f".getBytes(), "age".getBytes());
                String ageStr = Bytes.toString(ageBytes);
                int age = Integer.parseInt(ageStr);

                byte[] genderBytes = result.getValue("f".getBytes(), "gender".getBytes());
                String gender = Bytes.toString(genderBytes);

                if (age == 30 && "male".equals(gender)) {

                    // 如果静态画像也满足，则输出规则命中消息
                    message.put("user_id", userId);
                    message.put("match_time", userEvent.getEvent_time());

                    collector.collect(message.toJSONString());
                }
            }


        }

    }



}

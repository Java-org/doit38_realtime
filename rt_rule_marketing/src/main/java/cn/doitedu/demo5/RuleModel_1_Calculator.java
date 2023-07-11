package cn.doitedu.demo5;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
 * 规则模型1的模型运算机
 */
public class RuleModel_1_Calculator implements RuleCalculator {

    Table table;
    ValueState<Integer> cntState;
    JSONObject paramJsonObject;
    String ruleId;
    JSONObject message;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {

        // 获取一个查询离线画像条件的hbase连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "doitedu:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("user_profile"));

        // 获取一个用于动态画像统计的状态：用于统计一个事件的发生次数
        cntState = runtimeContext.getState(new ValueStateDescriptor<Integer>("cntState", Integer.class));

        // 解析规则的参数成一个JSONObject
        paramJsonObject = JSON.parseObject(ruleParamJson);

        ruleId = paramJsonObject.getString("rule_id");

        // 构造一个封装命中消息的对象
        message = new JSONObject();
        message.put("rule_id",ruleId);

    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        // 从规则参数json中，取出动态画像条件所要统计的eventId
        JSONObject onlineProfileJsonObject = paramJsonObject.getJSONObject("online_profile");
        String onlineProfileEventId = onlineProfileJsonObject.getString("event_id");
        // 从规则参数json中，取出动态画像条件所要统计的事件的最小发生次数
        Integer onlineProfileEventCount = onlineProfileJsonObject.getInteger("event_count");


        // 从规则参数json中，取出触发事件条件的各种参数： 事件id，属性名，属性值
        JSONObject fireEventJsonObject = paramJsonObject.getJSONObject("fire_event");
        String fireEventId = fireEventJsonObject.getString("event_id");
        String firePropName = fireEventJsonObject.getString("pro_name");
        String firePropValue = fireEventJsonObject.getString("pro_value");

        // 从规则参数json中，取出静态画像条件
        JSONArray offlineProfileJsonArray = paramJsonObject.getJSONArray("offline_profile");


        // 如果本次进来的行为id  = 动态画像要统计的行为id
        if (userEvent.getEvent_id().equals(onlineProfileEventId)) {
            // 次数+1
            cntState.update(cntState.value() == null ? 1 : cntState.value() + 1);
        }


        // 如果本次进来的行为 = 触发条件所要求的行为
        if (userEvent.getEvent_id().equals(fireEventId)
                && userEvent.getProperties().getOrDefault(firePropName, "").equals(firePropValue)
        ) {
            // 比对该用户的各种条件是否都已满足
            // 首先比对动态画像条件
            if (cntState.value() != null && cntState.value() >= onlineProfileEventCount) {

                // 再去查询静态画像条件是否满足
                // 先查询该用户的画像标签

                Get get = new Get(Bytes.toBytes(userEvent.getUser_id()));

                // 遍历静态画像条件数组中的每一个条件，取到其中的标签名，放入get查询条件中
                for (int i = 0; i < offlineProfileJsonArray.size(); i++) {
                    JSONObject jsonObject = offlineProfileJsonArray.getJSONObject(i);
                    String tagName = jsonObject.getString("tag_name");
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes(tagName));
                }

                Result result = table.get(get);
                // 遍历静态画像条件,挨个比对result中的值
                boolean isOk = true;
                for (int i = 0; i < offlineProfileJsonArray.size(); i++) {

                    JSONObject jsonObject = offlineProfileJsonArray.getJSONObject(i);
                    // 取出本次遍历到的静态条件的各种参数：标签名，比较类型，标签值
                    String tagName = jsonObject.getString("tag_name");
                    JSONArray tagValueArray = jsonObject.getJSONArray("tag_value");
                    String compareType = jsonObject.getString("compare_type");

                    // 取hbase查询结果中该标签的值
                    byte[] tagHbaseValue = result.getValue("f".getBytes(), tagName.getBytes());

                     isOk = compare(tagValueArray, compareType, Bytes.toString(tagHbaseValue));
                    if (!isOk) {
                        break;
                    }
                }

                // 如果isOk，说明条件全部成立，发消息
                if(isOk){
                    message.put("match_user",userEvent.getUser_id());
                    message.put("match_time",userEvent.getEvent_time());

                    collector.collect(message.toJSONString());

                }

            }

        }

    }

    private boolean compare(JSONArray tagValueArray, String compareType, String tagHbaseValue) {

        boolean isOk = false;

        switch (compareType) {
            case "=":
                String eqTargetValue = tagValueArray.getString(0);
                isOk = eqTargetValue.equals(tagHbaseValue);
                break;
            case ">":
                int gtTargetValue = tagValueArray.getInteger(0);
                // 将hbase查询结果值，转成数字类型
                double gtHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = gtHbaseValue > gtTargetValue;
                break;
            case ">=":
                int geTargetValue = tagValueArray.getInteger(0);
                // 将hbase查询结果值，转成数字类型
                double geHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = geHbaseValue >= geTargetValue;
                break;

            case "<":
                int ltTargetValue = tagValueArray.getInteger(0);
                // 将hbase查询结果值，转成数字类型
                double ltHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = ltHbaseValue < ltTargetValue;
                break;
            case "<=":
                int leTargetValue = tagValueArray.getInteger(0);
                // 将hbase查询结果值，转成数字类型
                double leHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = leHbaseValue <= leTargetValue;
                break;
            case "between":
                int btTargetValue_1 = tagValueArray.getInteger(0);
                int btTargetValue_2 = tagValueArray.getInteger(1);

                // 将hbase查询结果值，转成数字类型
                double btHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = btHbaseValue >= btTargetValue_1 && btHbaseValue <= btTargetValue_2;
                break;
        }


        return isOk;
    }


}

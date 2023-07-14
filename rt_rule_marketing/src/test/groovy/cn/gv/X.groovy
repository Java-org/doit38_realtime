
package cn.doitedu.demo9_doit39;

import cn.doitedu.demo9_doit39.beans.UserEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

/**
 * 规则模型1的模型运算机
 */
public class RuleModel_1_Calculator implements RuleCalculator {

    ValueState<Integer> cntState;
    JSONObject paramJsonObject;
    String ruleId;
    JSONObject message;
    Roaring64Bitmap targetUsers;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext, Roaring64Bitmap targetUsers) throws IOException {

        this.targetUsers = targetUsers;


        // 获取一个用于动态画像统计的状态：用于统计一个事件的发生次数
        cntState = runtimeContext.getState(new ValueStateDescriptor<Integer>("cntState", Integer.class));

        // 解析规则的参数成一个JSONObject
        paramJsonObject = JSON.parseObject(ruleParamJson);

        ruleId = paramJsonObject.getString("rule_id");

        // 构造一个封装命中消息的对象
        message = new JSONObject();
        message.put("rule_id", ruleId);

    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        // 判断该事件的行为人，是否属于该规则的目标人群,如果不是，则什么都不用做
        if (!targetUsers.contains(userEvent.getUser_id())) return;


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
                // 发营销消息
                message.put("match_user_id", userEvent.getUser_id());
                message.put("match_time", userEvent.getEvent_time());

                collector.collect(message.toJSONString());
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

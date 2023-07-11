package cn.doitedu.demo6;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class RuleModel2ModelCalculator implements RuleModelCalculator {

    String eventId;
    JSONObject message;
    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {
        // {"rule_id":"rule-002","event_id":"x"}
        JSONObject jsonObject = JSON.parseObject(ruleParamJson);
        eventId = jsonObject.getString("event_id");
        String ruleId = jsonObject.getString("rule_id");

        message = new JSONObject();
        message.put("rule_id",ruleId);

    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        if(userEvent.getEvent_id().equals(eventId)){
            message.put("user_id",userEvent.getUser_id());
            message.put("event_time",userEvent.getEvent_time());

            collector.collect(message.toJSONString());
        }
    }
}

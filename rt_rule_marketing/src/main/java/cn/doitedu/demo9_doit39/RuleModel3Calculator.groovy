package cn.doitedu.demo9_doit39

import cn.doitedu.demo9_doit39.beans.UserEvent
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.util.Collector
import org.roaringbitmap.longlong.Roaring64Bitmap

/*
 {
   "rule_id":"rule-4",
   "event_id":"add_cart"
 }
 */
class RuleModel3Calculator implements RuleCalculator{
    String eventId;
    JSONObject message;
    @Override
    void init(String ruleParamJson, RuntimeContext runtimeContext, Roaring64Bitmap targetUsers) throws IOException {
        JSONObject obj =  JSON.parseObject(ruleParamJson);
        String ruleId = obj.getString("rule_id");
        eventId = obj.getString("event_id");

        message = new JSONObject();
        message.put("match-rule-id",ruleId)
    }

    @Override
    void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {
        if(userEvent.getEvent_id().equals(eventId)){
            message.put("rule-match-time",userEvent.getEvent_time());
            message.put("rule-match-user",userEvent.getUser_id());
            collector.collect(message.toJSONString());
        }

    }
}

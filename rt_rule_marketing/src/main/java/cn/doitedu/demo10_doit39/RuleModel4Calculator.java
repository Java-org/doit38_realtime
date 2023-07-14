package cn.doitedu.demo10_doit39;

import cn.doitedu.demo10_doit39.beans.UserEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

/**
      {
         "online_profile": {
             "event_id": "w",
             "start_time":"2023-07-01 00:00:00",
             "end_time":"9999-12-31 00:00:00",
             "min_count":3,
             "c_id": 1
         },
         "fire_event_id":"x"
      }
 */
public class RuleModel4Calculator implements RuleCalculator{

    ValueState<Integer> cntState;
    String ruleId;
    JSONObject ruleParamJsonObject;
    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext, Roaring64Bitmap targetUsers) throws IOException {
        cntState = runtimeContext.getState(new ValueStateDescriptor<Integer>("cnt_state", Integer.class));

        ruleParamJsonObject = JSON.parseObject(ruleParamJson);
        ruleId = ruleParamJsonObject.getString("rule_id");

    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        // 收到一个用户行为，首先要判断，该行为的时间戳  是否  >  历史查询截止时间戳

        // 如果 成立

        // 看计数状态中是否有值

        // 如果计数状态中此刻还没有值，则去 redis中查询该规则，该条件，该用户  的 “历史值”

        // 然后，对查询到的 “历史值" +1 ，并放入计数状态




    }
}

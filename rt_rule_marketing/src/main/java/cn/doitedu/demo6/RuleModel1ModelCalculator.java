package cn.doitedu.demo6;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;


/**
 * 则 1 :
 * 静态画像条件： 画像标签 age>30 and age<40 AND gender=male
 * 实时动态画像标签条件： 在规则上线后，该用户发生 3次 w 行为
 * 规则触发条件： 发生x行为
 * <p>
 * 参数：
 * {
 * "ruleId":"rule-001",
 * "static_profile":[
 * {
 * "tag_name": "age",
 * "compare_op": "in",
 * "compare_value": [30,40]
 * },
 * {
 * "tag_name": "gender",
 * "compare_op": "=",
 * "compare_value": ["male"]
 * }
 * ],
 * "dynamic_profile": [
 * {
 * "event_id": "w",
 * "event_cnt": 3
 * }
 * ],
 * "fire_event": {
 * "event_id": "x",
 * "properties": [
 * {
 * "property_name": "p1",
 * "compare_op": "=",
 * "compare_value": "v1"
 * }
 * ]
 * }
 * }
 */
public class RuleModel1ModelCalculator implements RuleModelCalculator {

    JSONObject paramObject;
    MapState<Integer, Integer> state;

    Table table;

    JSONObject message;


    Roaring64Bitmap preSelectCrowd;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext , Roaring64Bitmap preSelectCrowd) throws IOException {
        //String ruleParam = "{\"ruleId\":\"rule-001\",\"static_profile\":[{\"tag_name\":\"age\",\"compare_op\":\"in\",\"compare_value\":[30,40]},{\"tag_name\":\"gender\",\"compare_op\":\"=\",\"compare_value\":[\"male\"]}],\"dynamic_profile\":[{\"event_id\":\"w\",\"event_cnt\":3}],\"fire_event\":{\"event_id\":\"x\",\"properties\":[{\"property_name\":\"p1\",\"compare_op\":\"=\",\"compare_value\":\"v1\"}]}}\n";

        // 解析参数
        paramObject = JSON.parseObject(ruleParamJson);

        // 申明状态  [ 条件id, 统计值 ]
        state = runtimeContext.getMapState(new MapStateDescriptor<Integer, Integer>("state", Integer.class, Integer.class));


        // 创建一个用于查询静态画像条件的  hbase 连接
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "doitedu:2181");

        Connection connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("user_profile"));


        message = new JSONObject();
        message.put("rule_id" , paramObject.getString("rule_id") );

        // demo6新增：  预圈选人群
        this.preSelectCrowd = preSelectCrowd;

    }

    @Override
    public void calculate(UserEvent userEvent , Collector<String> collector) throws Exception {

        // 只有当事件行为人，属于本规则的 预圈选人群 ，才对他的行为做 动态画像统计 或  触发判断  处理
        if(preSelectCrowd.contains(userEvent.getUser_id())) {
            // 首先，对输入的事件，进行动态画像统计处理
            dynamicProfileProcess(userEvent);

            fireProcess(userEvent, collector);
        }
    }

    private void fireProcess(UserEvent userEvent, Collector<String> collector) throws Exception {

        /**
         * 1. 检查输入事件是否是规则要求的  触发行为事件
         */
        JSONObject fireEventObject = paramObject.getJSONObject("fire_event");
        String fireEventId = fireEventObject.getString("event_id");
        JSONArray propertiesArray = fireEventObject.getJSONArray("properties");

        if (userEvent.getEvent_id().equals(fireEventId)) {

            boolean match = true;
            for (int i = 0; i < propertiesArray.size(); i++) {
                // 取出第i个属性参数对象
                JSONObject propertyParamObject_i = propertiesArray.getJSONObject(i);

                // 取出属性条件对象中要求的： 属性名，属性值，比较关系
                String propertyName = propertyParamObject_i.getString("property_name");
                String compareOp = propertyParamObject_i.getString("compare_op");
                String compareValue = propertyParamObject_i.getString("compare_value");

                // 把接收到的事件中，该属性的值取出来
                String myValue = userEvent.getProperties().get(propertyName);

                switch (compareOp) {
                    case "=":
                        match = compareValue.equals(myValue);
                        break;
                    case ">":
                        match = Double.parseDouble(myValue) > Double.parseDouble(compareValue);
                        break;
                    case "<":
                        match = Double.parseDouble(myValue) < Double.parseDouble(compareValue);
                        break;
                    default:
                        throw new RuntimeException("谁让你用乱七八糟的比较运算符来着，我不干活了");

                }
                if (!match) break;
            }

            // 如果经过检查，输入事件不属于规则要求的触发事件，则直接返回了；
            if(!match) return;

            /**
             * 2. 检查该用户的动态画像条件是否满足
             */
            JSONArray dynamicProfileArray = paramObject.getJSONArray("dynamic_profile");

            for (int i = 0; i < dynamicProfileArray.size(); i++) {
                JSONObject profile_i = dynamicProfileArray.getJSONObject(i);
                Integer flagId = profile_i.getInteger("flag_id");
                Integer eventCnt = profile_i.getInteger("event_cnt");

                Integer stateCntValue = state.get(flagId);
                if (stateCntValue == null || stateCntValue < eventCnt) {
                    match = false;
                    return;
                }
            }

            // 上述一系列检查如果都通过了,则输出规则命中消息
            if(match){
                message.put("user_id", userEvent.getUser_id());
                message.put("event_time", userEvent.getEvent_time());

                collector.collect(message.toJSONString());
            }
        }
    }

    private void dynamicProfileProcess(UserEvent userEvent) throws Exception {
        // 从参数中获取动态画像条件所要统计的事件
        JSONArray dynamicProfileArray = paramObject.getJSONArray("dynamic_profile");

        // 遍历动态画像条件数组
        for (int i = 0; i < dynamicProfileArray.size(); i++) {

            // 取出1个动态画像条件对象
            JSONObject dynamicProfile_i = dynamicProfileArray.getJSONObject(i);

            // 取出该画像条件中要统计的事件id
            String eventId_i = dynamicProfile_i.getString("event_id");

            // 取出该画像条件的标识id号
            Integer flagId = dynamicProfile_i.getInteger("flag_id");

            // 判断接收到的用户行为，是否是画像条件所要统计的行为
            if (userEvent.getEvent_id().equals(eventId_i)) {
                // 如果是目标统计事件，则对它的次数更新
                Integer oldValue = state.get(flagId);
                state.put(flagId, oldValue == null ? 1 : oldValue + 1);
            }
        }
    }


}

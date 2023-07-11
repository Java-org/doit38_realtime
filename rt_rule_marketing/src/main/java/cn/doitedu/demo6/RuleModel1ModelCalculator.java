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

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {
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

    }

    @Override
    public void calculate(UserEvent userEvent , Collector<String> collector) throws Exception {

        // 首先，对输入的事件，进行动态画像统计处理
        dynamicProfileProcess(userEvent);

        fireProcess(userEvent, collector);

    }

    private void fireProcess(UserEvent userEvent, Collector<String> collector) throws Exception {
        // 先从规则参数中，提取 触发条件的 参数
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


            // 如果经过上述的 eventId  和  properties 比较之后
            // match 为  : true
            // 则说明该用户事件，确实是本规则要求的触发行为
            // 就要正式判断该用户，是否已经完全满足该规则的 动态画像条件 和    静态画像条件

            // 先从规则参数中，取到动态画像条件的参数
            JSONArray dynamicProfileArray = paramObject.getJSONArray("dynamic_profile");

            boolean judgeFlag = true;
            for (int i = 0; i < dynamicProfileArray.size(); i++) {
                JSONObject profile_i = dynamicProfileArray.getJSONObject(i);
                Integer flagId = profile_i.getInteger("flag_id");
                Integer eventCnt = profile_i.getInteger("event_cnt");

                Integer stateCntValue = state.get(flagId);
                if (stateCntValue == null || stateCntValue < eventCnt) {
                    judgeFlag = false;
                    break;
                }
            }


            // 进而判断该用户的 静态画像条件是否满足
            JSONArray staticProfileArray = paramObject.getJSONArray("static_profile");

            // 拼接hbase的get参数
            Get get = new Get(Bytes.toBytes(userEvent.getUser_id()));
            for (int i = 0; i < staticProfileArray.size(); i++) {
                JSONObject staticProfile_i = staticProfileArray.getJSONObject(i);
                String tagName = staticProfile_i.getString("tag_name");
                get.addColumn("f".getBytes(), tagName.getBytes());
            }

            // 调用hbase客户端查询 画像标签值
            Result result = table.get(get);
            match = true;
            for (int i = 0; i < staticProfileArray.size(); i++) {

                JSONObject staticProfile_i = staticProfileArray.getJSONObject(i);
                String tagName = staticProfile_i.getString("tag_name");
                String compareOp = staticProfile_i.getString("compare_op");
                // 条件中要求的 判断阈值
                String compareValue = staticProfile_i.getString("compare_value");

                // hbase中查出来的真实值
                byte[] tagValueBytes = result.getValue("f".getBytes(), tagName.getBytes());
                String tagValue = Bytes.toString(tagValueBytes);


                // 比较两个值
                switch (compareOp) {
                    case "=":
                        match = compareValue.equals(tagValue);
                        break;
                    case ">":
                        match = Double.parseDouble(tagValue) > (Double.parseDouble(compareValue));
                        break;
                    case "<":
                        match = Double.parseDouble(tagValue) < (Double.parseDouble(compareValue));
                        break;
                    default:
                        throw new RuntimeException("带给你一个消息");
                }

                // 但凡有一个条件不满足，则跳出循环
                if(!match) break;
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

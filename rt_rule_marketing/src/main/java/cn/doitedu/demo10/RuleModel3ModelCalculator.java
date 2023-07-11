package cn.doitedu.demo10;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/16
 * @Desc: 学大数据，上多易教育
 * <p>
 * 模型3的运算机类，支持跨上线时间点前后时间段的动态画像统计条件
 **/
public class RuleModel3ModelCalculator implements RuleModelCalculator {

    Jedis jedis;
    ValueState<Integer> eventCountState;
    Long dynamicProfileHistoryQueryEndTime;
    JSONObject paramJsonObject;
    JSONObject message;
    String ruleId;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext, Roaring64Bitmap crowdBitmap) throws IOException {
        // 构建 redis 客户端连接
        jedis = new Jedis("doitedu", 6379);

        // 申请一个进行 动态画像统计的 状态
        eventCountState = runtimeContext.getState(new ValueStateDescriptor<Integer>("event-cnt", Integer.class));

        // 获取参数中携带的  动态画像统计历史值查询 截止时间点
        paramJsonObject = JSON.parseObject(ruleParamJson);
        dynamicProfileHistoryQueryEndTime = paramJsonObject.getLong("dynamic_profile_history_query_end");

        // 构造一个输出命中消息的对象
        ruleId = paramJsonObject.getString("rule_id");
        message = new JSONObject();
        message.put("rule_id", ruleId);
    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

       // demo8新增:  需要判断 flink 传入的事件数据，是否是  历史值统计截止点之后的数据
        if (userEvent.getEvent_time() > dynamicProfileHistoryQueryEndTime) {
            // 动态画像处理
            dynamicProfileProcess(userEvent);

            // 触发判断处理
            fireMatchProcess(userEvent, collector);
        }

    }

    private void dynamicProfileProcess(UserEvent userEvent) throws IOException, ParseException {

        // 从参数中取出动态画像条件的信息
        JSONArray dynamicProfileArray = paramJsonObject.getJSONArray("dynamic_profile");

        for (int i = 0; i < dynamicProfileArray.size(); i++) {
            JSONObject dynamicProfile_i = dynamicProfileArray.getJSONObject(i);
            Integer flagId = dynamicProfile_i.getInteger("flag_id");
            String eventId = dynamicProfile_i.getString("event_id");
            String endTimeStr = dynamicProfile_i.getString("end_time");

            long endTime = Long.MAX_VALUE;
            if (StringUtils.isNotBlank(endTimeStr)) {
                Date date = DateUtils.parseDate(endTimeStr, "yyyy-MM-dd HH:mm:ss");
                endTime = date.getTime();
            }

            // 判断当前输入的事件，是否为本条件要求的事件
            if (eventId.equals(userEvent.getEvent_id()) && userEvent.getEvent_time() <= endTime) {
                // 去为这个条件指标累加发生次数

                // 先取出该用户该条件的旧值
                Integer oldValue = eventCountState.value();

                // 如果oldValue为null，说明该用户的该条件还没有统计过，需要去redis中查询历史时段统计值
                if (oldValue == null) {
                    // redis中存了  :  x规则的 y条件的u用户的：历史值
                    // 我们用hash结构:   大 key:  规则id|条件id
                    //                       hash:  [user_id,  历史值]
                    String redisHistValue = jedis.hget(ruleId + "|" + flagId, userEvent.getUser_id() + "");

                    oldValue = redisHistValue == null ? 0 : Integer.parseInt(redisHistValue);
                }


                // 将oldValue + 1 ，更新到统计状态中
                eventCountState.update(oldValue + 1);
            }


        }
    }


    // 触发判断
    private void fireMatchProcess(UserEvent userEvent, Collector<String> collector) throws IOException {
        JSONObject fireEvent = paramJsonObject.getJSONObject("fire_event");
        String fireEventId = fireEvent.getString("event_id");

        JSONArray dynamicProfileArray = paramJsonObject.getJSONArray("dynamic_profile");

        // 如果输入事件 == 规则触发事件
        if (userEvent.getEvent_id().equals(fireEventId)) {

            boolean isMatch = true;
            // 遍历判断每一个动态画像条件是否满足
            for (int i = 0; i < dynamicProfileArray.size(); i++) {

                JSONObject dynamicProfile_i = dynamicProfileArray.getJSONObject(i);
                Integer eventCnt = dynamicProfile_i.getInteger("event_cnt");
                Integer flagId = dynamicProfile_i.getInteger("flag_id");

                // 从状态中取出该用户的该条件的统计次数
                Integer realCnt = eventCountState.value();
                // 如果为空，则说明还没被统计过，则要去redis中查他的历史值，并更新到state
                if(realCnt == null) {
                    String redisHistValue = jedis.hget(ruleId + "|" + flagId, userEvent.getUser_id() + "");
                    realCnt = redisHistValue == null ? 0 : Integer.parseInt(redisHistValue);
                    eventCountState.update(realCnt);
                }


                if (realCnt < eventCnt) {
                    isMatch = false;
                    break;
                }

            }

            // 如果isMatch == true,则说明上面的所有动态画像统计条件皆满足
            if (isMatch) {
                message.put("user_id", userEvent.getUser_id());
                message.put("match_time", userEvent.getEvent_time());
                collector.collect(message.toJSONString());
            }


        }
    }

}

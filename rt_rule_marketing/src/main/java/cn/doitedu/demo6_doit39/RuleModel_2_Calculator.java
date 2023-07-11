package cn.doitedu.demo6_doit39;

import cn.doitedu.demo6_doit39.beans.Model2OfflineProfileCondition;
import cn.doitedu.demo6_doit39.beans.Model2Param;
import cn.doitedu.demo6_doit39.beans.UserEvent;
import com.alibaba.fastjson.JSON;
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

public class RuleModel_2_Calculator implements RuleCalculator {

    Table table ;

    ValueState<Integer> indexState;
    ValueState<Integer> countState;

    Model2Param model2Param;
    JSONObject message;

    @Override
    public void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException {

        // 获取一个查询离线画像条件的hbase连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "doitedu:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("user_profile"));

        // 获取一个记录序列所到达的索引号的状态
        indexState = runtimeContext.getState(new ValueStateDescriptor<Integer>("indexState", Integer.class));
        countState = runtimeContext.getState(new ValueStateDescriptor<Integer>("countState", Integer.class));

        // 将传入的参数，解析成 参数封装对象
        model2Param = JSON.parseObject(ruleParamJson, Model2Param.class);

        message = new JSONObject();
        message.put("rule_id",model2Param.getRule_id());

    }

    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        Integer curIndex = indexState.value();
        if(curIndex == null ) curIndex = 0;

        // 看当前事件，是否是序列中此刻索引号的事件
        if(userEvent.getEvent_id().equals( model2Param.getOnline_profile().getEvent_seq()[curIndex])){
            // 如果是，则索引号递增,以便于下次要找的就是序列中的下一个了
            int nextIndex = curIndex+1;
            if(nextIndex == 3){
                // 指针回零
                nextIndex = 0;
                // 发生次数+1
                countState.update(countState.value() == null? 1: countState.value()+1);
            }

            indexState.update(nextIndex);
        }

        // 看当前事件，是否是触发事件
        // 先看动态画像条件是否满足
        if(countState.value()!=null && countState.value()>= model2Param.getOnline_profile().getSeq_count()){
            // 再判断静态画像条件是否都满足
            Get get = new Get(Bytes.toBytes(userEvent.getUser_id()));

            for (Model2OfflineProfileCondition model2OfflineProfileCondition : model2Param.getOffline_profile()) {
                String tagName = model2OfflineProfileCondition.getTag_name();
                get.addColumn("f".getBytes(),tagName.getBytes());
            }

            Result result = table.get(get);

            boolean isOk = true;
            for (Model2OfflineProfileCondition condition : model2Param.getOffline_profile()) {
                String tagName = condition.getTag_name();
                String compareType = condition.getCompare_type();
                String[] targetValue = condition.getTag_value();

                // 比较目标值和查询出来的值是否匹配
                byte[] hbaseValue = result.getValue("f".getBytes(), tagName.getBytes());
                isOk = compare(targetValue,compareType,Bytes.toString(hbaseValue));
                if(!isOk){
                    break;
                }
            }

            if(isOk){

                message.put("match_time",userEvent.getEvent_time());
                message.put("match_user",userEvent.getUser_id());
                collector.collect(message.toJSONString());
            }

        }



    }


    private boolean compare(String[] targetValueArray, String compareType, String tagHbaseValue) {

        boolean isOk = false;

        switch (compareType) {
            case "=":
                String eqTargetValue = targetValueArray[0];
                isOk = eqTargetValue.equals(tagHbaseValue);
                break;
            case ">":
                String gtTargetValueStr = targetValueArray[0];
                double gtTargetValue = Double.parseDouble(gtTargetValueStr);

                // 将hbase查询结果值，转成数字类型
                double gtHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = gtHbaseValue > gtTargetValue;
                break;
            case ">=":
                String geTargetValueStr = targetValueArray[0];
                double geTargetValue = Double.parseDouble(geTargetValueStr);
                // 将hbase查询结果值，转成数字类型
                double geHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = geHbaseValue >= geTargetValue;
                break;

            case "<":
                String ltTargetValueStr = targetValueArray[0];
                double ltTargetValue = Double.parseDouble(ltTargetValueStr);
                // 将hbase查询结果值，转成数字类型
                double ltHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = ltHbaseValue < ltTargetValue;
                break;
            case "<=":
                String leTargetValueStr = targetValueArray[0];
                double leTargetValue = Double.parseDouble(leTargetValueStr);
                // 将hbase查询结果值，转成数字类型
                double leHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = leHbaseValue <= leTargetValue;
                break;
            case "between":
                String btTargetValue_1Str = targetValueArray[0];
                double btTargetValue_1 = Double.parseDouble(btTargetValue_1Str);
                String btTargetValue_2Str = targetValueArray[1];
                double btTargetValue_2 = Double.parseDouble(btTargetValue_2Str);

                // 将hbase查询结果值，转成数字类型
                double btHbaseValue = Double.parseDouble(tagHbaseValue);
                isOk = btHbaseValue >= btTargetValue_1 && btHbaseValue <= btTargetValue_2;
                break;
        }


        return isOk;
    }


}

package cn.doitedu.demo4;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.io.IOException;

public interface RuleCalculator {

    /**
     * 规则运算机的 初始化入口
     */
    void init(String ruleParamJson, RuntimeContext runtimeContext) throws IOException;


    /* *
     * 规则运算机的 事件处理入口
     */
    void calculate(UserEvent userEvent, Collector<String> collector) throws Exception;

}

package cn.doitedu.demo9_doit39;

import cn.doitedu.demo9_doit39.beans.UserEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

public interface RuleCalculator {

    /**
     * 规则运算机的 初始化入口
     */
    void init(String ruleParamJson, RuntimeContext runtimeContext, Roaring64Bitmap targetUsers) throws IOException;


    /* *
     * 规则运算机的 事件处理入口
     */
    void calculate(UserEvent userEvent, Collector<String> collector) throws Exception;

}

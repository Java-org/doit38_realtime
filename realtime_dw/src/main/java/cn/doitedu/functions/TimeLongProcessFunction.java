package cn.doitedu.functions;

import cn.doitedu.beans.TimelongBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimeLongProcessFunction extends KeyedProcessFunction<String, TimelongBean, TimelongBean> {

    ValueState<TimelongBean> beanState;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 申请一个状态存储，用来记录一条bean
        beanState = getRuntimeContext().getState(new ValueStateDescriptor<TimelongBean>("bean_state", TimelongBean.class));

    }

    @Override
    public void processElement(TimelongBean timelongBean, KeyedProcessFunction<String, TimelongBean, TimelongBean>.Context context, Collector<TimelongBean> collector) throws Exception {
        long currentTime = timelongBean.getEvent_time();
        TimelongBean stateBean = beanState.value();


        if("app_launch".equals(timelongBean.getEvent_id())){
            return;
        }

        if( stateBean == null ) {
            // 设置 页面起始、结束时间
            timelongBean.setPage_start_time(currentTime);
            timelongBean.setPage_end_time(currentTime);
            // 放入状态
            beanState.update(timelongBean);
            stateBean = beanState.value();
        }

        // page_load事件，需要将上一个页面的结束时间更新成本事件时间，并输出成一条虚拟事件
        else if(timelongBean.getEvent_id().equals("page_load")) {
            stateBean.setPage_end_time(currentTime);
            // 额外立刻输出
            collector.collect(stateBean);

            // 开启一个新页面的开始
            stateBean.setPage_url(timelongBean.getPage_url());   // 新的页面地址
            stateBean.setPage_start_time(currentTime);  // 新的起始时间

        }

        // wakeup事件，需要 “开启一个页面（起始时间、结束时间更新）”
        else if("wake_up".equals(timelongBean.getEvent_id())){
            stateBean.setPage_start_time(currentTime);
            stateBean.setPage_end_time(currentTime);
        }

        // 普通事件，更新endTime
        else {
            stateBean.setPage_end_time(currentTime);
        }

        collector.collect(stateBean);

    }
}

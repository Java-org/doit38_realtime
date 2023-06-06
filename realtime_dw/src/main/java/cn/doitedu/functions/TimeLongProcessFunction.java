package cn.doitedu.functions;

import cn.doitedu.beans.TimelongBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimeLongProcessFunction extends KeyedProcessFunction<String, TimelongBean, TimelongBean> {

    ValueState<TimelongBean> beanState;
    ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 申请一个状态存储，用来记录一条bean
        beanState = getRuntimeContext().getState(new ValueStateDescriptor<TimelongBean>("bean_state", TimelongBean.class));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));

    }

    @Override
    public void processElement(TimelongBean timelongBean, KeyedProcessFunction<String, TimelongBean, TimelongBean>.Context context, Collector<TimelongBean> collector) throws Exception {
        long currentTime = timelongBean.getEvent_time();
        TimelongBean stateBean = beanState.value();


        // 判断此前是否有注册过定时器
        if (timerState.value() != null) {
            // 删除原来的定时器
            context.timerService().deleteProcessingTimeTimer(timerState.value());
        }

        // 注册一个新的 会话idle超时 时长定时器
        long timerTime = context.timerService().currentProcessingTime() + 10 * 1000L;
        context.timerService().registerProcessingTimeTimer(timerTime);
        // 并将定时器的时间记录到状态中
        timerState.update(timerTime);


        if ("app_launch".equals(timelongBean.getEvent_id())) {
            return;
        }

        if (stateBean == null) {
            // 设置 页面起始、结束时间
            timelongBean.setPage_start_time(currentTime);
            timelongBean.setPage_end_time(currentTime);
            // 放入状态
            beanState.update(timelongBean);
            stateBean = beanState.value();
        }

        // page_load事件，需要将上一个页面的结束时间更新成本事件时间，并输出成一条虚拟事件
        else if (timelongBean.getEvent_id().equals("page_load")) {
            stateBean.setPage_end_time(currentTime);
            // 额外立刻输出
            collector.collect(stateBean);

            // 开启一个新页面的开始
            stateBean.setPage_url(timelongBean.getPage_url());   // 新的页面地址
            stateBean.setPage_start_time(currentTime);  // 新的起始时间

        }

        // wakeup事件，需要 “开启一个页面（起始时间、结束时间更新）”
        else if ("wake_up".equals(timelongBean.getEvent_id())) {
            stateBean.setPage_start_time(currentTime);
            stateBean.setPage_end_time(currentTime);
        }

        // 普通事件，更新endTime
        else {
            stateBean.setPage_end_time(currentTime);
        }


        // 输出数据
        collector.collect(stateBean);


        if ("app_close".equals(timelongBean.getEvent_id())) {
            // 明确结束会话，则应该清理状态
            beanState.clear();
        }

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, TimelongBean, TimelongBean>.OnTimerContext ctx, Collector<TimelongBean> out) throws Exception {

        // 清除状态
        beanState.clear();
        timerState.clear();
        System.out.println("状态被清除了....");

    }
}

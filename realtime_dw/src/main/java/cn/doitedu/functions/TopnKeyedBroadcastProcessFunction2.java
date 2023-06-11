package cn.doitedu.functions;

import cn.doitedu.beans.BrandTopnBean;
import cn.doitedu.beans.ItemCdcInnerBean;
import cn.doitedu.beans.OrderCdcInnerBean;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class TopnKeyedBroadcastProcessFunction2 extends KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, BrandTopnBean> {


    MapState<Long, ItemCdcInnerBean> itemsState;
    ValueState<Long> timerState;



    @Override
    public void open(Configuration parameters) throws Exception {

        // item记录的主键id -> item记录
        itemsState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ItemCdcInnerBean>("items", Long.class, ItemCdcInnerBean.class));

        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));


    }

    /**
     * 处理主流数据（商品购买记录）
     */
    @Override
    public void processElement(ItemCdcInnerBean itemBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, BrandTopnBean>.ReadOnlyContext readOnlyContext, Collector<BrandTopnBean> collector) throws Exception {

        // 1,p1,o1,耐克,5,100
        // 2,p2,o1,耐克,2,200
        // 3,p1,o2,耐克,1,500
        // 4,p6,o2,耐克,2,200

        // 上述的数据，来一条就先存入状态，此时不做任何计算
        itemsState.put(itemBean.getId(), itemBean);


        // 注册起始定时器
        Long timerTime = timerState.value();
        if (timerTime == null) {
            timerTime = (readOnlyContext.currentProcessingTime() / 60000) * 60000 + 60000;
            readOnlyContext.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);
        }


    }

    /**
     * 处理广播流数据（订单主表数据）
     */
    @Override
    public void processBroadcastElement(OrderCdcInnerBean orderBean, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, BrandTopnBean>.Context context, Collector<BrandTopnBean> collector) throws Exception {

        BroadcastState<Long, OrderCdcInnerBean> broadcastState = context.getBroadcastState(new MapStateDescriptor<>("order-state", Long.class, OrderCdcInnerBean.class));
        broadcastState.put(orderBean.getId(), orderBean);

        // 将收到的订单主表数据，放入广播状态（新增，覆盖）

        // 细节：源头进来的数据是cdc数据，那么一个订单就可能进来多次
        // 1,未支付   | c
        // 1,已支付   | u
        // 1,已支付   | u  4.8状态  4.9到达
        // 1,已取消   | u  4.9状态  5.2到达
        // 不管中间有过多少次订单状态转移，当我们的计算触发时间点到达时，就关心此刻的最终状态 1-5
    }


    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, ItemCdcInnerBean, OrderCdcInnerBean, BrandTopnBean>.OnTimerContext ctx, Collector<BrandTopnBean> out) throws Exception {

        String productBrand = ctx.getCurrentKey();

        long static_start_time = timestamp - 60000;
        long static_end_time = timestamp;

        ReadOnlyBroadcastState<Long, OrderCdcInnerBean> broadcastState = ctx.getBroadcastState(new MapStateDescriptor<>("order-state", Long.class, OrderCdcInnerBean.class));

        // 对商品购买明细，遍历，按相同商品聚合
        // 1,p1,o1,耐克,5,100
        // 2,p2,o1,耐克,2,200
        // 3,p1,o2,耐克,1,500
        // 4,p6,o2,耐克,2,200

        HashMap<Long, BigDecimal> aggMap = new HashMap<>();

        // 遍历每条明细，进行累加聚合
        Iterable<Map.Entry<Long, ItemCdcInnerBean>> entries = itemsState.entries();
        for (Map.Entry<Long, ItemCdcInnerBean> entry : entries) {
            ItemCdcInnerBean itemBean = entry.getValue();

            String brand = itemBean.getProduct_brand();
            long productId = itemBean.getProduct_id();
            long orderId = itemBean.getOrder_id();
            int quantity = itemBean.getProduct_quantity();
            BigDecimal price = itemBean.getProduct_price();

            OrderCdcInnerBean orderBean = broadcastState.get(orderId);
            int status = orderBean.getStatus();
            long paymentTime = orderBean.getPayment_time();

            // 如果该商品购买明细所属的订单是已经支付了，且支付时间在本次统计的时间窗口内，则要对该条明细中的金额进行累加
            if ((status == 1 || status == 2 || status == 3) && paymentTime >= static_start_time && paymentTime < static_end_time) {
                BigDecimal oldValue = aggMap.getOrDefault(productId, BigDecimal.ZERO);
                BigDecimal newValue = oldValue.add(price.multiply(BigDecimal.valueOf(quantity)));
                aggMap.put(productId, newValue);
            }
        }


        for (Map.Entry<Long, BigDecimal> aggEntry : aggMap.entrySet()) {
            out.collect(new BrandTopnBean(productBrand, aggEntry.getKey(), aggEntry.getValue(), static_start_time, static_end_time));
        }


        // 对aggMap中的每个商品，按照它的支付总额 倒序排序，并输出 topn

    }
}

package cn.doitedu.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.sql.Timestamp;

/*
    订单总数、总额 、应付总额 （当日新订单）
    待支付订单数、订单额  （当日新订单）
    支付订单数、订单额  （当日支付）
    发货订单数、订单额  （当日发货）
    完成订单数、订单额  （当日确认）
 */
@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderDiffValues {

    private String metricFlag = null;
    private Timestamp outTime = null;

    private int totalCount = 0;
    private BigDecimal totalOriginAmount = BigDecimal.ZERO;
    private BigDecimal totalRealAmount = BigDecimal.ZERO;

    private int toPayTotalCount = 0;
    private BigDecimal toPayTotalAmount= BigDecimal.ZERO;

    private int payedTotalCount = 0;
    private BigDecimal payedTotalAmount = BigDecimal.ZERO;

    private int deliveredTotalCount = 0;
    private BigDecimal deliveredTotalAmount = BigDecimal.ZERO;

    private int confirmedTotalCount = 0;
    private BigDecimal confirmedTotalAmount = BigDecimal.ZERO;


    public void clear(){
        this.outTime = null;
        this.metricFlag = null;
        this.totalCount = 0;
        this.totalOriginAmount = BigDecimal.ZERO;
        this.totalRealAmount = BigDecimal.ZERO;

        this.toPayTotalCount = 0;
        this.toPayTotalAmount= BigDecimal.ZERO;

        this.payedTotalCount = 0;
        this.payedTotalAmount = BigDecimal.ZERO;

        this.deliveredTotalCount = 0;
        this.deliveredTotalAmount = BigDecimal.ZERO;

        this.confirmedTotalCount = 0;
        this.confirmedTotalAmount = BigDecimal.ZERO;
    }
}

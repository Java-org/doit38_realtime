package cn.doitedu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;


@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCdcInnerBean {
    private long id;
    private BigDecimal total_amount;
    private BigDecimal pay_amount;
    private int status;
    private int confirm_status;
    private long create_time;
    private long payment_time;
    private long delivery_time;
    private long receive_time;
    private long modify_time;



}

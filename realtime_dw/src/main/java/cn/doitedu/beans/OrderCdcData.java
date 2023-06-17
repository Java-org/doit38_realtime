package cn.doitedu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.sql.Timestamp;


@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCdcData {
    private Long id;
    private BigDecimal total_amount;
    private BigDecimal pay_amount;
    private int status;
    private Timestamp create_time;
    private Timestamp payment_time;
    private Timestamp delivery_time;
    private Timestamp confirm_time;
    private Timestamp update_time;
}

package cn.doitedu.beans;

import lombok.*;

import java.math.BigDecimal;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Setter
public class ProductAmount {

    private long productId;
    private long orderId;
    private BigDecimal productAmount;

}

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
public class ItemCdcInnerBean {
    private long id;
    private long order_id;
    private long product_id;
    private String product_name;
    private String product_brand;
    private int product_quantity;
    private BigDecimal product_price;
}

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
public class BrandTopnBean {
    private String brand;
    private long product_id;
    //private String product_name;
    private BigDecimal product_amt;
    private long static_start_time;
    private long static_end_time;
}

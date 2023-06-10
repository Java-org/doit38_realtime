package cn.doitedu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCdcOuterBean {

    OrderCdcInnerBean before;
    OrderCdcInnerBean after;

    // r / u / c / d
    String op;
}

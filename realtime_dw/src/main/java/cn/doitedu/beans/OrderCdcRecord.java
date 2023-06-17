package cn.doitedu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCdcRecord {

    OrderCdcData before;
    OrderCdcData after;

    // r / u / c / d
    String op;
}

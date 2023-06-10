package cn.doitedu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemCdcOuterBean {
    private ItemCdcInnerBean before;
    private ItemCdcInnerBean after;
    // r / u / c / d
    private String op;
}

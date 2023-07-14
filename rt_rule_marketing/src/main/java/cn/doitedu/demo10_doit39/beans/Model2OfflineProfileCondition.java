package cn.doitedu.demo10_doit39.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Model2OfflineProfileCondition {
    private String tag_name;
    private String compare_type;
    private String[] tag_value;

}

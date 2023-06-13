package cn.doitedu.demo5;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RuleMetaBean {
    private String op;
    private String ruleId;
    private String ruleModelId;
    private String ruleParamJson;

}

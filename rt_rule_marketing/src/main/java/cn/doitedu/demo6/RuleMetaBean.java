package cn.doitedu.demo6;


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
    private int onlineStatus;

}

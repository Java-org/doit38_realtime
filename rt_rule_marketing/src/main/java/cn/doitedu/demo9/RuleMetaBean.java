package cn.doitedu.demo9;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.roaringbitmap.longlong.Roaring64Bitmap;

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

    // demo6新增的字段  : 预圈选人群
    private Roaring64Bitmap preSelectedCrowd;

    // demo8: 跨时段的动态画像条件 历史值统计截止时间点
    private Long dynamicProfileHistoryQueryEndTime;

    // demo9: 新增规则元数据携带的  运算机类源码
    private String calculatorGroovyCode;


}

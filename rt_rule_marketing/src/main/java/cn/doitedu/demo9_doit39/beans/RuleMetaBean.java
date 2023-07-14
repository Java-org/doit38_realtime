package cn.doitedu.demo9_doit39.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Data
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class RuleMetaBean {

    // 元数据表的操作类型，就是cdc的row中的RowKind
    private int operateType;
    // 规则id
    private String ruleId;
    // 规则的参数
    private String paramJson;

    // 规则所属的模型的运算机源代码
    private String calculatorSourceCode;

    // 规则的管理状态
    private int status;

    //
    private Roaring64Bitmap targetUsers;

}

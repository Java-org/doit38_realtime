package cn.doitedu.demo7;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/6/14
 * @Desc: 学大数据，上多易教育
 *
 *   人工模拟，带预圈选人群的 规则定义
 *
 **/
public class TestDemo6_RuleMetaMoni {

    public static void main(String[] args) throws Exception {

        String paramJson1 = "{\"rule_id\":\"rule-001\",\"static_profile\":[{\"tag_name\":\"age\",\"compare_op\":\"in\",\"compare_value\":[30,40]},{\"tag_name\":\"gender\",\"compare_op\":\"=\",\"compare_value\":[\"male\"]}],\"dynamic_profile\":[{\"flag_id\":1,\"event_id\":\"w\",\"event_cnt\":3}],\"fire_event\":{\"event_id\":\"x\",\"properties\":[{\"property_name\":\"p1\",\"compare_op\":\"=\",\"compare_value\":\"v1\"}]}}";
        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit38", "root", "root");

        PreparedStatement pst = connection.prepareStatement("insert into rule_meta values(?,?,?,?,?)");
        pst.setString(1,"rule-001");
        pst.setString(2,"model-001");
        pst.setString(3,paramJson1);
        pst.setInt(4,2);

        // 预圈选人群
        Roaring64Bitmap crowdBitmap = Roaring64Bitmap.bitmapOf(1, 3, 5, 7, 10, 12);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        crowdBitmap.serialize(new DataOutputStream(bout));

        byte[] crowdBitmapBytes = bout.toByteArray();
        pst.setBytes(5,crowdBitmapBytes);

        // 执行insert语句
        pst.execute();


    }
}

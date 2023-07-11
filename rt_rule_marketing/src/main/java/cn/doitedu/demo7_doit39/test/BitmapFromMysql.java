package cn.doitedu.demo7_doit39.test;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;

public class BitmapFromMysql {
    public static void main(String[] args) throws SQLException, IOException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit39", "root", "root");

        PreparedStatement preparedStatement = conn.prepareStatement("select rule_id,target_users from rule_meta where rule_id = ? ");
        preparedStatement.setString(1,"rule-4");

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();

        String ruleId = resultSet.getString("rule_id");
        byte[] targetUsersBytes = resultSet.getBytes("target_users");

        // 反序列化成bitmap对象
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(targetUsersBytes));

        // 就可以用它来判断某人是否属于该人群
        System.out.println(bitmap.contains(1));
        System.out.println(bitmap.contains(2));
        System.out.println(bitmap.contains(3));


    }
}

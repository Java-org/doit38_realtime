package cn.doitedu.demo7_doit39.test;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.*;

public class Bitmap2Mysql {

    public static void main(String[] args) throws SQLException, IOException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit39", "root", "root");
        PreparedStatement preparedStatement = conn.prepareStatement("insert into rule_meta values(?,?,?,?,?,?,?)");
        preparedStatement.setString(1,"rule-4");
        preparedStatement.setString(2,"{}");
        preparedStatement.setInt(3,1);


        // 用es客户端，根据规则参数里面的静态画像条件，去es服务器查询人群
        // 假如查到的人群有   1,3,5,7,9

        // 这些人放入bitmap
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(1, 3, 5, 7, 9);
        // 序列化这个bitmap成为一个字节数组
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        bitmap.serialize(dataOutputStream);
        byte[] byteArray = byteArrayOutputStream.toByteArray();

        preparedStatement.setBytes(4,byteArray);
        preparedStatement.setString(5,"深似海的男人");
        preparedStatement.setTimestamp(6,new Timestamp(System.currentTimeMillis()));
        preparedStatement.setTimestamp(7,new Timestamp(System.currentTimeMillis()));

        // 执行这个填好数据的sql语句
        preparedStatement.execute();



    }


}

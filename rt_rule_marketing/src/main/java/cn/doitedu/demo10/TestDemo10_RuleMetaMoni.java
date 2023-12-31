package cn.doitedu.demo10;

import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.*;

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
public class TestDemo10_RuleMetaMoni {

    @Test
    public  void model1Rule() throws SQLException, IOException {

        String paramJson1 = "{\n" +
                "  \"rule_id\": \"rule-001-002\",\n" +
                "  \"static_profile\": [\n" +
                "    {\n" +
                "      \"tag_name\": \"age\",\n" +
                "      \"compare_op\": \">\",\n" +
                "      \"compare_value\": 20\n" +
                "    },\n" +
                "    {\n" +
                "      \"tag_name\": \"gender\",\n" +
                "      \"compare_op\": \"=\",\n" +
                "      \"compare_value\": \"female\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"dynamic_profile\": [\n" +
                "    {\n" +
                "      \"flag_id\": 1,\n" +
                "      \"event_id\": \"k\",\n" +
                "      \"event_cnt\": 4\n" +
                "    }\n" +
                "  ],\n" +
                "  \"fire_event\": {\n" +
                "    \"event_id\": \"m\",\n" +
                "    \"properties\": [\n" +
                "      {\n" +
                "        \"property_name\": \"p1\",\n" +
                "        \"compare_op\": \"=\",\n" +
                "        \"compare_value\": \"v2\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}\n";
        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit38", "root", "root");

        /**
         * 根据规则参数中所指定的模型id，去模型表中找到运算机源码
         */
        PreparedStatement queryPst = connection.prepareStatement("select calculator_code from model_meta where model_id = ?");
        queryPst.setString(1,"model-001");
        ResultSet resultSet = queryPst.executeQuery();
        resultSet.next();
        String calculatorCode = resultSet.getString("calculator_code");


        PreparedStatement pst = connection.prepareStatement("insert into rule_meta values(?,?,?,?,?,?,?)");
        pst.setString(1,"rule-001-002");
        pst.setString(2,"model-001");
        pst.setString(3,paramJson1);
        pst.setInt(4,2);

        // 预圈选人群
        Roaring64Bitmap crowdBitmap = Roaring64Bitmap.bitmapOf(2, 4, 6, 8, 10);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        crowdBitmap.serialize(new DataOutputStream(bout));

        byte[] crowdBitmapBytes = bout.toByteArray();
        pst.setBytes(5,crowdBitmapBytes);

        // 历史值截止点
        pst.setLong(6,Long.MIN_VALUE);

        // 设置模型的运算机代码
        pst.setString(7,calculatorCode);

        // 执行insert语句
        pst.execute();


    }


    @Test
    public  void model2Rule() throws SQLException, IOException {

        String paramJson1 = "{\n" +
                "  \"rule_id\": \"rule-002\",\n" +
                "  \"event_id\": \"p\"\n" +
                "}";
        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit38", "root", "root");

        PreparedStatement pst = connection.prepareStatement("insert into rule_meta values(?,?,?,?,?,?)");
        pst.setString(1,"rule-002");
        pst.setString(2,"model-002");
        pst.setString(3,paramJson1);
        pst.setInt(4,2);

        // 预圈选人群 -- 其实本规则不需要
        Roaring64Bitmap crowdBitmap = Roaring64Bitmap.bitmapOf();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        crowdBitmap.serialize(new DataOutputStream(bout));

        byte[] crowdBitmapBytes = bout.toByteArray();
        pst.setBytes(5,crowdBitmapBytes);


        // 历史值截止点
        pst.setLong(6,Long.MIN_VALUE);


        // 执行insert语句
        pst.execute();


    }


    @Test
    public  void model3Rule() throws SQLException, IOException {

        String paramJson1 = "{\n" +
                "  \"rule_id\": \"rule-003-001\",\n" +
                "  \"dynamic_profile\": [\n" +
                "    {\n" +
                "      \"flag_id\": 1,\n" +
                "      \"event_id\": \"w\",\n" +
                "      \"event_cnt\": 3,\n" +
                "      \"start_time\": \"2023-06-01 10:00:00\",\n" +
                "      \"end_time\" : \"\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"fire_event\": {\n" +
                "    \"event_id\": \"q\"\n" +
                "  }\n" +
                "}";
        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit38", "root", "root");

        PreparedStatement pst = connection.prepareStatement("insert into rule_meta values(?,?,?,?,?,?,?)");
        pst.setString(1,"rule-003-001");
        pst.setString(2,"model-003");
        pst.setString(3,paramJson1);
        pst.setInt(4,2);

        /**
         * 根据规则参数中所指定的模型id，去模型表中找到运算机源码
         */
        PreparedStatement queryPst = connection.prepareStatement("select calculator_code from model_meta where model_id = ?");
        queryPst.setString(1,"model-003");
        ResultSet resultSet = queryPst.executeQuery();
        resultSet.next();
        String calculatorCode = resultSet.getString("calculator_code");

        pst.setString(7,calculatorCode);



        // 预圈选人群 -- 其实本规则不需要
        Roaring64Bitmap crowdBitmap = Roaring64Bitmap.bitmapOf();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        crowdBitmap.serialize(new DataOutputStream(bout));

        byte[] crowdBitmapBytes = bout.toByteArray();
        pst.setBytes(5,crowdBitmapBytes);


        // 历史值截止点
        long histQueryEnd = System.currentTimeMillis();
        // 拼接通sql
        //  select  user_id,count(1) FILTER(where event_id = 参数要的event_id)
        //  from   dwd.user_events where event_time >= 参数的start_time  and  event_time <= histQueryEnd
        // group by user_id
        pst.setLong(6,histQueryEnd);

        // 将历史值发布到redis去

        // 假设上面的查询得到如下结果：
        // 1 -> 发生了2次
        // 3 -> 发生了1次
        // 5 -> 发生了6次

        Jedis jedis = new Jedis("doitedu", 6379);
        jedis.hset("rule-003-001|1","1","2");
        jedis.hset("rule-003-001|1","3","1");
        jedis.hset("rule-003-001|1","5","6");
        jedis.close();


        // 执行insert语句
        pst.execute();


    }


}

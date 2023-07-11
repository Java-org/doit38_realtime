import cn.doitedu.demo5.Model2Param;
import com.alibaba.fastjson.JSON;

public class TestJsonParse {
    public static void main(String[] args) {


        String json = "{\n" +
                "  \"rule_id\":\"rule-04\",\n" +
                "  \"offline_profile\":[\n" +
                "    {\n" +
                "      \"tag_name\":\"active_level\",\n" +
                "      \"compare_type\":\"=\",\n" +
                "      \"tag_value\":[2]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tag_name\":\"gender\",\n" +
                "      \"compare_type\":\"=\",\n" +
                "      \"tag_value\":[\"male\"]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"online_profile\":{\n" +
                "    \"event_seq\":[\"k\",\"b\",\"c\"],\n" +
                "    \"seq_count\":3\n" +
                "  },\n" +
                "  \"fire_event\":{\n" +
                "    \"event_id\":\"share\",\n" +
                "    \"pro_name\":\"share_method\",\n" +
                "    \"pro_value\":\"qq\"\n" +
                "  }\n" +
                "}";

        Model2Param model2Param = JSON.parseObject(json, Model2Param.class);
        System.out.println(model2Param.getRule_id());
        System.out.println(model2Param.getOffline_profile());
        System.out.println(model2Param.getOnline_profile());
        System.out.println(model2Param.getFire_event());


    }


}

package cn.doitedu.batch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.sql.*;

public class StaticProfilePreSelectDemo {

    public static void main(String[] args) throws IOException, SQLException {


        RoaringBitmap tagSelectUsersBitmap = RoaringBitmap.bitmapOf();
        RoaringBitmap dorisSelectUsersBitmap = RoaringBitmap.bitmapOf();


        // es的请求客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient
                        .builder(new HttpHost("doitedu", 9200, "http"))
        );

        // 用于查询参数封装的对象
        SearchRequest request = new SearchRequest("doit39_profile");

        /**
         * 单一条件查询
         */
        // 定义一个 范围查询条件
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("tag0101").gt(300);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("tag0301", "中国");
        /*MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("tag0203", "江苏省");*/

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder
                .must(rangeQueryBuilder)
                .must(matchQueryBuilder);
                /*.must(matchQueryBuilder1);*/


        // 将查询条件参数，封装成查询请求
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));

        // 用客户端发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //response.getHits().forEach(ht-> System.out.println(ht.getId()));

        /// 遍历搜索结果，将返回id放入bitmap
        response.getHits().forEach(ht-> {
            System.out.println(Integer.parseInt(ht.getId()));
            tagSelectUsersBitmap.add(Integer.parseInt(ht.getId()));
        });

        /*System.out.println(tagSelectUsersBitmap.toString());*/

        System.out.println("---------------分割线----------------");

        /**
         * 组合条件查询
         */
        // 定义一个 条件组装器
        /*BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // 定义一个 范围条件
        RangeQueryBuilder ageCondition = QueryBuilders.rangeQuery("tag0202").gt(26).lte(30);

        // 定义一个分词匹配条件
        TermQueryBuilder genderCondition = QueryBuilders.termQuery("tag0201", "female");

        MatchQueryBuilder interestingTerms = QueryBuilders.matchQuery("tag0301", "明月");


        // 组装查询条件
        BoolQueryBuilder queryBuilder = boolQueryBuilder.must(ageCondition).must(genderCondition).must(interestingTerms);

        // 将查询条件参数，封装成查询请求
        request.source(new SearchSourceBuilder().query(queryBuilder));

        // 用客户端发送请求
        SearchResponse response2 = client.search(request, RequestOptions.DEFAULT);
        //response2.getHits().forEach(ht -> System.out.println(ht.getId()));
        response2.getHits().forEach(ht -> tagSelectUsersBitmap.add(Integer.parseInt(ht.getId())));

        System.out.println(tagSelectUsersBitmap.toString());
        System.out.println(tagSelectUsersBitmap.contains(6));
        System.out.println(tagSelectUsersBitmap.contains(7));
        System.out.println(tagSelectUsersBitmap.getCardinality());


        // 下面是从doris中查询 另外一个条件：  add_cart >= 3  (统计窗口一定是截止在规则上线之前）、
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:9030/dwd", "root", "123456");
        PreparedStatement stmt = conn.prepareStatement("select user_id from dwd.user_event_detail where event_time between (?,?) and event_id = ? group by user_id having count(1)>=?");
        stmt.setString(1, "2023-04-01 00:00:00");
        stmt.setString(2, "2023-04-15 00:00:00");
        stmt.setString(3, "add_cart");
        stmt.setInt(4, 3);

        ResultSet resultSet = stmt.executeQuery();
        while (resultSet.next()) {
            int user_id = resultSet.getInt("user_id");
            dorisSelectUsersBitmap.add(user_id);
        }

        // 合并每一个条件所得bitmap作为最终的预圈选人群bitmap
        tagSelectUsersBitmap.and(dorisSelectUsersBitmap);*/

        // 再将这个最终 人群bitmap ： tagSelectUsersBitmap ，跟随规则的其他字段，写入规则元数据表

        client.close();

    }


}



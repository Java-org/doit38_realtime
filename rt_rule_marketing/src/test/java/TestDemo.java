import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestDemo {

    public static void main(String[] args) throws IOException {

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "doitedu:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table1 = connection.getTable(TableName.valueOf("user_profile"));

        Get get = new Get(Bytes.toBytes(1L));
        Result result = table1.get(get);
        System.out.println(result.getValue("f".getBytes(),"age".getBytes()));


        org.apache.hadoop.conf.Configuration configuration2 = HBaseConfiguration.create();
        configuration2.set("hbase.zookeeper.quorum", "doitedu:2181");
        Connection connection2 = ConnectionFactory.createConnection(configuration);
        Table table2 = connection.getTable(TableName.valueOf("user_profile"));
        Get get2 = new Get(Bytes.toBytes(1L));
        Result result2 = table2.get(get2);
        System.out.println(result2.getValue("f".getBytes(),"age".getBytes()));



    }

}

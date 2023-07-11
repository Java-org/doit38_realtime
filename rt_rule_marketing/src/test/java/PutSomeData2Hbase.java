import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutSomeData2Hbase {

    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "doitedu:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("user_profile"));

        Put put1 = new Put(Bytes.toBytes(1L));
        put1.addColumn("f".getBytes(),"age".getBytes(),"25".getBytes());
        put1.addColumn("f".getBytes(),"gender".getBytes(),"male".getBytes());

        Put put2 = new Put(Bytes.toBytes(2L));
        put2.addColumn("f".getBytes(),"age".getBytes(),"35".getBytes());
        put2.addColumn("f".getBytes(),"gender".getBytes(),"male".getBytes());

        table.put(put1);
        table.put(put2);


        Get get = new Get(Bytes.toBytes(1));
        get.addColumn("f".getBytes(),"age".getBytes());

        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getValue("f".getBytes(),"age".getBytes())));

    }

}

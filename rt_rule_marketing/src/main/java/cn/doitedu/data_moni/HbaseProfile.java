package cn.doitedu.data_moni;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HbaseProfile {

    public static void main(String[] args) throws IOException {

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","doitedu:2181");

        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf("user_profile"));

        // 插入数据
        Put put1 = new Put(Bytes.toBytes(1L));
        put1.addColumn("f".getBytes(),"gender".getBytes(),"male".getBytes());
        put1.addColumn("f".getBytes(),"active_level".getBytes(),Bytes.toBytes("3"));
        put1.addColumn("f".getBytes(),"age".getBytes(),Bytes.toBytes("30"));


        Put put2 = new Put(Bytes.toBytes(2L));
        put2.addColumn("f".getBytes(),"gender".getBytes(),"male".getBytes());
        put2.addColumn("f".getBytes(),"active_level".getBytes(),Bytes.toBytes("2"));
        put2.addColumn("f".getBytes(),"age".getBytes(),Bytes.toBytes("30"));


        Put put3 = new Put(Bytes.toBytes(3L));
        put3.addColumn("f".getBytes(),"gender".getBytes(),"female".getBytes());
        put3.addColumn("f".getBytes(),"active_level".getBytes(),Bytes.toBytes("3"));
        put3.addColumn("f".getBytes(),"age".getBytes(),Bytes.toBytes("28"));


        Put put4 = new Put(Bytes.toBytes(4L));
        put4.addColumn("f".getBytes(),"gender".getBytes(),"female".getBytes());
        put4.addColumn("f".getBytes(),"active_level".getBytes(),Bytes.toBytes("2"));
        put4.addColumn("f".getBytes(),"age".getBytes(),Bytes.toBytes("32"));


        Put put5 = new Put(Bytes.toBytes(5L));
        put5.addColumn("f".getBytes(),"gender".getBytes(),"female".getBytes());
        put5.addColumn("f".getBytes(),"active_level".getBytes(),Bytes.toBytes("2"));
        put5.addColumn("f".getBytes(),"age".getBytes(),Bytes.toBytes("20"));

        List<Put> putList = Arrays.asList(put1, put2, put3, put4,put5);

        table.put(putList);

        table.close();
        connection.close();

    }


}

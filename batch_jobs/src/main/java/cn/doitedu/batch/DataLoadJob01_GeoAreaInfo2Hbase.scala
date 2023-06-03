package cn.doitedu.batch

import ch.hsr.geohash.GeoHash
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * 从 mysql中加载原始的地理位置参考信息表，加工成 :  geohash码,省,市,区
 * 并把结果rdd,利用mapreduce的TableOutputFormat组件，写入hbase
 */
object DataLoadJob01_GeoAreaInfo2Hbase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions","1")
    val spark = SparkSession.builder()
      .appName("gps地域维表etl任务")
      .master("local")
      .config(conf)
      .getOrCreate()


    /**
     * 从mysql中读取原始数据表，并注册成视图
     */
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    val df = spark.read.jdbc("jdbc:mysql://doitedu:3306/realtimedw", "t_md_areas", props)
    df.createTempView("t")

    val reverseGeoFunc = (lat: Double, lng: Double) => {
      // 得到geohash编码，并将其反转
      GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5).reverse
    }
    spark.udf.register("geo", reverseGeoFunc)

    /**
     * 对原始数据中的地理位置父子结构进行加工，得到扁平结构：  geohash->省,市,区
     */
    val resDf:DataFrame = spark.sql(
      """
        |select
        |   geohash,
        |   province,
        |   city,
        |   nvl(region,'') as region
        |from(
        |   select
        |      geohash,
        |      province,
        |      city,
        |      region,
        |      -- 利用row_number()over() 对 相同重复的数据 进行去重
        |      row_number() over(partition by geohash order by province) as rn
        |   from
        |   (
        |      -- 对原始地理位置表，进行自关联，将层级数据扁平化
        |      SELECT
        |        geo(lv4.BD09_LAT, lv4.BD09_LNG) as geohash,
        |        lv1.AREANAME as province,
        |        lv2.AREANAME as city,
        |        lv3.AREANAME as region
        |      from t lv4
        |        join t lv3 on lv4.`LEVEL`=4 and lv4.bd09_lat is not null and lv4.bd09_lng is not null and lv4.PARENTID = lv3.ID
        |        join t lv2 on lv3.PARENTID = lv2.ID
        |        join t lv1 on lv2.PARENTID = lv1.ID
        |   ) o1
        |) o2
        |
        |where rn=1 and geohash is not null and length(trim(geohash))=5
        |and province is not null and trim(province)!=''
        |and city is not null and trim(city)!=''
        |
        |""".stripMargin)




    // 将加工好的数据，写入hbase
    val toHbaseRdd: RDD[(ImmutableBytesWritable, Put)] = resDf.rdd.map(row => {
      val geoHashCode = row.getAs[String]("geohash")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val region = row.getAs[String]("region")

      // 将本行数据封装到hbase的数据插入对象Put中
      val put = new Put(geoHashCode.getBytes())
      put.addColumn("f".getBytes(), "p".getBytes(), province.getBytes())
      put.addColumn("f".getBytes(), "c".getBytes(), city.getBytes())
      put.addColumn("f".getBytes(), "r".getBytes(), region.getBytes())

      // 形成key-value两元组结构
      (new ImmutableBytesWritable(geoHashCode.getBytes()), put)
    })


    /**
     * 配置调用 hbase-mapreduce api的参数
     */
    val sc = spark.sparkContext
    // hbase 目标表名
    val tableName = "dim_geo_area"

    // hbase集群所连的zookeeper配置信息
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "doitedu")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    // 为sparkContext设置outputFormat为hbase的TableOutputFormat
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 封装mapreduce的Job配置信息
    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    /**
     * 调用kvRDD上的saveAsNewAPIHadoopDataset()将rdd数据写出到hbase
     */
    toHbaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    spark.close()
  }

}

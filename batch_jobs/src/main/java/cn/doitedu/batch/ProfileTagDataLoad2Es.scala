package cn.doitedu.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

object ProfileTagDataLoad2Es {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "doitedu")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")


    val spark = SparkSession.builder()
      .appName("烧成灰我都能写")
      .master("local")
      .config(conf)
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()

    // 读hive表
    val df = spark.sql(
      """
        |with tmp1 as (
        |  select * from dws.user_profile_01 where dt='2023-04-16'
        |),
        |tmp2 as (
        |  select * from dws.user_profile_02 where dt='2023-04-16'
        |),
        |tmp3 as (
        |  select * from dws.user_profile_03 where dt='2023-04-16'
        |)
        |
        |select
        | tmp1.user_id,
        | tmp1.tag0101,
        | tmp1.tag0102,
        | tmp1.tag0103,
        | tmp1.tag0104,
        | tmp2.tag0201,
        | tmp2.tag0202,
        | tmp2.tag0203,
        | tmp2.tag0204,
        | tmp2.tag0205,
        | tmp3.tag0301
        |
        |from tmp1
        |LEFT JOIN  tmp2  ON tmp1.user_id = tmp2.user_id
        |LEFT JOIN  tmp3  ON tmp1.user_id = tmp3.user_id
        |
        |""".stripMargin)


    // 一句话，将spark整理好的dataframe数据，写入elastic search
    EsSparkSQL.saveToEs(df, "doit39_profile", Map("es.mapping.id" -> "user_id"));


    spark.close()
  }
}

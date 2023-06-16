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
        |
        |select
        | p01.user_id,
        | p01.tag0101,
        | p01.tag0102,
        | p01.tag0103,
        | p01.tag0104,
        | p02.tag0201,
        | p02.tag0202,
        | p02.tag0203,
        | p02.tag0204,
        | p02.tag0205,
        | p03.tag0301
        |
        |from dws.user_profile_01 p01
        |LEFT JOIN  dws.user_profile_02 p02  ON p01.user_id = p02.user_id
        |LEFT JOIN  dws.user_profile_03 p03  ON p01.user_id = p03.user_id
        |
        |""".stripMargin)


    // 一句话，将spark整理好的dataframe数据，写入elastic search
    EsSparkSQL.saveToEs(df, "doit38_profile", Map("es.mapping.id" -> "user_id"));


    spark.close()
  }
}

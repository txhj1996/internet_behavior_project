package com.anryg.bigdata.streaming.sparkbatch

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession};

/**
 * @DESC: 从kafka读取上网数据,写入hive动态分区表
 *
 */
object SparkBatchFromHiveCoalease {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hj")
    val conf = new SparkConf()
      .setAppName("StructuredStreamingFromKafka2Hive")
      .setMaster("local[*]") //本地运行模式，如果提交集群，注释掉这行
    val spark = SparkSession.builder().config(conf)
      //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
      //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hadoop102:10000")
      //                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hadoop102:9083")
      .config("hive.metastore.uris", "thrift://hadoop102:9083")
      .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
      .getOrCreate()

    val df: DataFrame = spark.sql(
      """
        |select
        |	*
        |from
        |	dwd.dwd_internetlog_detail_full
        | where year=2022 and month=07 and day=28
        |""".stripMargin
    ).repartition(9)
    df.write.partitionBy("year", "month", "day").format("orc").mode("append").saveAsTable("dwd.dwd_internetlog_detail_full_coal")
    //    df.write.saveAsTable("dwd.dwd_internetlog_detail")


  }

}

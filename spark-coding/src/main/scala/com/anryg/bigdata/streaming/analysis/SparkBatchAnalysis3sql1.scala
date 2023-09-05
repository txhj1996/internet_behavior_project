package com.anryg.bigdata.streaming.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @DESC: 分析上网次数最多的ip所上的网站
 * @Auther: hj
 */
object SparkBatchAnalysis3sql1 {

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

    spark.sql(
      """
        |select
        |	domain,count(*) cnt
        |from
        |	dwd.dwd_internetlog_detail_coal
        | where country ="中国" and day = 29
        |group by
        |	domain
        |order by
        |	cnt desc
        |limit 20
        |""".stripMargin
    ).rdd.coalesce(1).saveAsTextFile("output18")


   
    spark.stop()

  }

}

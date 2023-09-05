package com.anryg.bigdata.streaming.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @DESC: 分析上网次数最多的ip，以及所上的网站
 * @Auther: hj
 */
object SparkBatchAnalysis1Sql {

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
        |	  target_ip,count(*) cnt
        |from
        |	dwd.dwd_internetlog_detail_full_coal
        |where target_ip="1.180.234.207" or target_ip="106.117.213.103" or target_ip="101.71.100.227" or target_ip="23.101.24.70"
        |group by target_ip
        |order by cnt desc
        | limit 10;
        |""".stripMargin
    ).write.format("csv").save("output5")


    //    spark.sql(
    //      """
    //        |select
    //        |	*
    //        |from
    //        |	ods.ods_kafka_internetlog
    //        |where year=2022 and month=07 and day=29
    //        |order by time,client_ip, target_ip
    //        |limit 20;
    //        |""".stripMargin
    //    ).show()
    //          write.format("csv").save("output5")
    //    spark.sql(
    //      """
    //        |select
    //        |	*
    //        |from
    //        |	test.test
    //        |where year=2022 and month=07 and day=29
    //        |order by time desc
    //        |limit 20;
    //        |""".stripMargin
    //    ).write.format("csv").save("output4")
    spark.stop()

  }

}

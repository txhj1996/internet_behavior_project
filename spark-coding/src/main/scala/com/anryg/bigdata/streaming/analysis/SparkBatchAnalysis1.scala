package com.anryg.bigdata.streaming.analysis

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util

/**
 * @DESC: 分析上网次数最多的ip
 * @Auther: hj
 */
object SparkBatchAnalysis1 {

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
        |	  client_ip
        |from
        |	dwd.dwd_internetlog_detail_coal
        |""".stripMargin
    )
    val rdd: RDD[Row] = df.rdd
    val mapRdd: RDD[(String, Int)] = rdd.map(row => {
      (row.getString(0), 1)
    })
    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)

    val res: Array[(String, Int)] = reduceRdd.sortBy(_._2,ascending = false).take(10)
    res.foreach(println)


//    val groupRdd: RDD[(String, Iterable[String])] = mapRdd.groupByKey()
//    val value: RDD[(String, Int, util.HashMap[String, Int])] = groupRdd.map {
//      case (client_ip, domainArray) => {
//        val size: Int = domainArray.size
//        val map = new util.HashMap[String, Int]()
//        domainArray.foreach(domain => {
//          map.put(domain, map.getOrDefault(domain, 0) + 1)
//        })
//        (client_ip, size, map)
//      }
//    }
//    val sortRdd: RDD[(String, Int, util.HashMap[String, Int])] = value.sortBy(_._2)
//    val res: Array[(String, Int, util.HashMap[String, Int])] = sortRdd.take(10)
//    res.foreach(println)

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

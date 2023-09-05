package com.anryg.bigdata.streaming.sparkbatch

import com.anryg.bigdata.IpSearch
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis;

/**
 * @DESC: 从kafka读取上网数据,写入hive动态分区表
 *
 */
object SparkBatchFromOdsToDwdTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hj")
    val conf = new SparkConf()
      .setAppName("SparkBatchFromOdsToDwd")
          .setMaster("local[*]") //本地运行模式，如果提交集群，注释掉这行
    val spark = SparkSession.builder().config(conf)
      //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
      //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hadoop102:10000")
      //                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hadoop102:9083")
      .config("hive.metastore.uris", "thrift://hadoop102:9083")
      .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
      .getOrCreate()
    //注意：虽然hive里设置了，但是spark也要设置
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    import spark.implicits._

    val df: DataFrame = spark.table("ods.ods_kafka_internetlog");

    df.write.format("csv").save("output4")


  }

}

package com.anryg.bigdata.streaming.sparkbatch

import com.anryg.bigdata.{IpSearch, RedisClientUtils, RedisClientUtils1}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

import java.util.Arrays;

/**
 * @DESC: 从kafka读取上网数据,写入hive动态分区表
 *
 */
object SparkBatchFromOdsToDwd {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hj")
    val conf = new SparkConf()
      .setAppName("SparkBatchFromOdsToDwd")
//              .setMaster("local[*]") //本地运行模式，如果提交集群，注释掉这行
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

    val df: DataFrame = spark.sql(
      """
        |select
        |	*
        |from
        |	ods.ods_kafka_internetlog
        | where length(target_ip) >= 7
        |""".stripMargin
    ).map(row => {
      val clientIP: String = row.getAs[String]("client_ip")
      val targetIP = row.getAs[String]("target_ip")
      val jedis = new Jedis("hadoop102", 6379)
      jedis.select(15)
      val ipAndAddr: Array[String] = IpSearch.getAddrByIP(jedis, clientIP).split("-")
      val t_ipAndAddr: Array[String] = IpSearch.getAddrByIP(jedis, targetIP ).split("-")
      jedis.close()
      val country: String = ipAndAddr(2)
      val t_country: String = t_ipAndAddr(2)
      val province: String = ipAndAddr(3)
      val t_province: String = t_ipAndAddr(3)
      val city: String = ipAndAddr(4)
      val t_city: String = t_ipAndAddr(4)
      val operator: String = ipAndAddr(5)
      val t_operator: String = t_ipAndAddr(5)
      val domain: String = row.getAs[String]("domain").toLowerCase //将域名转成小写
      val time = row.getAs[String]("time")

      val rcode = row.getAs[String]("rcode")
      val queryType = row.getAs[String]("query_type")
      val authRecord = row.getAs[String]("authority_record").toLowerCase
      val addMsg = row.getAs[String]("add_msg")
      val dnsIP = row.getAs[String]("dns_ip")
      val year = row.getAs[String]("year")
      val month = row.getAs[String]("month")
      val day = row.getAs[String]("day")
      (clientIP, country, province, city, operator, domain, time, targetIP,t_country, t_province, t_city, t_operator, rcode, queryType, authRecord, addMsg, dnsIP, year, month, day)
    })
      .toDF("client_ip", "country", "province", "city", "operator", "domain",
        "time", "target_ip", "t_country", "t_province", "t_city", "t_operator","rcode", "query_type", "authority_record", "add_msg", "dns_ip", "year", "month", "day")

    df.write.partitionBy("year", "month", "day").format("orc").mode("append").saveAsTable("dwd.dwd_internetlog_detail_full")
    //    df.write.saveAsTable("dwd.dwd_internetlog_detail")
    //val split: Array[String] = IpSearch.getAddrByIP(RedisClientUtils.getSingleRedisClient, "1.0.0.255").split("-")
    //    split.foreach(println)


  }

}

package com.anryg.bigdata.streaming.analysis

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @DESC: 分析上网次数最多的ip所上的网站
 * @Auther: hj
 */
object SparkBatchAnalysis2 {

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
        |	  client_ip,domain
        |from
        |	dwd.dwd_internetlog_detail_coal
        |""".stripMargin
    )
    val rdd: RDD[Row] = df.rdd

    val filterRdd: RDD[Row] = rdd.filter(row => {
      val client_ip: String = row.getString(0)
      client_ip.equals("111.0.166.79") || client_ip.equals("85.0.231.87") || client_ip.equals("111.0.236.187") || client_ip.equals("211.0.172.197") || client_ip.equals("111.0.154.205") || client_ip.equals("85.0.247.67") || client_ip.equals("111.0.194.103") || client_ip.equals("85.0.176.151") || client_ip.equals("111.0.231.84") || client_ip.equals("85.0.205.162")
    })

    //(ip1,a) ==>((ip1,a),1),((ip2,b),1),((ip2,b),1)
    val mapRdd: RDD[((String, String), Int)] = filterRdd.map(row => {
      ((row.getString(0), row.getString(1)), 1)
    }
    )
    //((ip1,a),1),((ip1,b),1)，((ip2,b),1),((ip2,b),1)  ==>((ip1,a),1),((ip1,b),1)，((ip2,b),2)
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    //((ip1,a),1),((ip1,b),1)，((ip2,b),2) ==> (ip1,(a,1)),(ip1,(b,1)),(ip2,(b,2))
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((client_ip, domain), count) => (client_ip, (domain, count))
    }
    //(ip1,(a,1)),(ip1,(b,1)),(ip2,(b,2))==> (ip1,{(a,1),(b,1)}),(ip2,{(b,2)})=
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()

    val res: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })

    res.coalesce(1).saveAsTextFile("output7")


   
    spark.stop()

  }

}

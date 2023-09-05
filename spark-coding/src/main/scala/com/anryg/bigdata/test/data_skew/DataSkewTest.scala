package com.anryg.bigdata.test.data_skew

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
  * @DESC: 一个数据倾斜的例子
  *
  */
object DataSkewTest {

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

        val rawRdd: RDD[Row] = spark.sql(
            """
              |select
              |	  client_ip,target_ip
              |from
              |	dwd.dwd_internetlog_detail_full_coal
              |where target_ip="1.180.234.207" or target_ip="106.117.213.103" or target_ip="101.71.100.227" or target_ip="23.101.24.70"
              |""".stripMargin
        ).rdd

        val mapRdd: RDD[(String, String)] = rawRdd.map(row => {
            /** 根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总 */

            val target_ip = row.getString(1)
            val client_ip = row.getString(0)
            (target_ip, client_ip)
        })

        val groupRdd: RDD[(String, Iterable[String])] = mapRdd.groupByKey(4)



        val targetRDD = groupRdd.map(kv => {/**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
            val map = new util.HashMap[String,Int]()
            val target_ip = kv._1
            val clientIPArray = kv._2
            clientIPArray.foreach(clientIP => {
                map.put(clientIP,map.getOrDefault(clientIP,0)+1)
//                if (map.containsKey(clientIP)) {
//                    val sum = map.get(clientIP) + 1
//                    map.put(clientIP,sum)
//                }
//                else map.put(clientIP,1)
            })
            (target_ip,map)
        })

        targetRDD.saveAsTextFile("outputskew") //结果数据保存目录




        spark.stop()
    }
}

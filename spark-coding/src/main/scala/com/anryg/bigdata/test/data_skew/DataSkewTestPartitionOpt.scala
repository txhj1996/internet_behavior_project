package com.anryg.bigdata.test.data_skew

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @DESC: 一个数据倾斜的例子
  *
  */
object DataSkewTestPartitionOpt {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hj")
        val conf = new SparkConf()
          .setAppName("StructuredStreamingFromKafka2Hive")
//          .setMaster("local[*]") //本地运行模式，如果提交集群，注释掉这行
        val spark = SparkSession.builder().config(conf)
          //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
          //                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hadoop102:10000")
          //                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hadoop102:9083")
          .config("hive.metastore.uris", "thrift://hadoop102:9083")
          .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
          .getOrCreate()

        val rdd: RDD[Row] = spark.sql(
            """
              |select
              |	  country
              |from
              |	dwd.dwd_internetlog_detail_full_coal
              |""".stripMargin
        ).rdd

      val mapRdd: RDD[(String, Int)] = rdd.map(row => {
        /** 根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总 */


        val country = row.getString(0)
        (country, 1)
      })

      val groupRdd: RDD[(String, Iterable[Int])] = mapRdd.groupByKey(new MyPartitioner(3))

      groupRdd.map(kv =>{
        (kv._1,kv._2.size)
      }).saveAsTextFile("output77")


        spark.stop()
    }
}

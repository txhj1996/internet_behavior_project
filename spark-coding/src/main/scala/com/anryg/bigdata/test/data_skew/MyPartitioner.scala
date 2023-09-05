package com.anryg.bigdata.test.data_skew

import org.apache.spark.Partitioner

/**
  * @DESC: 实现自定义的分区策略
  *
  */
class MyPartitioner(partitionNum: Int) extends Partitioner{
    override def numPartitions: Int = partitionNum  //确定总分区数量

    override def getPartition(key: Any): Int = {//确定数据进入分区的具体策略
        val keyStr = key.toString
        if (keyStr == "中国") {
            0
        } else if (keyStr == "日本") {
            1
        } else {
            // 如果键不是 "中国" 或 "日本"，可以使用默认的分区算法
            2
        }
    }
}

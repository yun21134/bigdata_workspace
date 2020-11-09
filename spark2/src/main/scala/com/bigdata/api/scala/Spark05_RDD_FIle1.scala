package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark05_RDD_FIle1
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark05_RDD_FIle1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("koca").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    // totalsize = 7
    // partitions = 2
    // minsize = 7 / 2 = 3
    // p0 (3) + p1(3) + p2(1)
    // spark文件分区取决于hadoop文件切片规则

    val lineRDD: RDD[String] = sc.textFile("data/4.txt",2)

    // 4 byte / 2p = 2

    lineRDD.saveAsTextFile("output1")
    // 9 / 3 = > 3
  }

}

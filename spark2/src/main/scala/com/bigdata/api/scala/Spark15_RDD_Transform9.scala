package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark15_RDD_Transform9
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark15_RDD_Transform9 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform9").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1)))

    val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new org.apache.spark.HashPartitioner(4))

    // K_V类型的算子的源码都不在RDD中，通过隐式转换在PairRDDFunctions源码中查找
    // 转换算子 - partitionBy
    // 可以通过指定的分区器决定数据计算的分区,spark默认的分区器为HashPartitioner
    val rdd3: RDD[(Int, (Int, String))] = rdd2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    rdd3.collect().foreach(println)

    sc.stop()


  }


}

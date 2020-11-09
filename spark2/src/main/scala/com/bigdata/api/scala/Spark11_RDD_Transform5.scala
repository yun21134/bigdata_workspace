package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark09_RDD_Transform3
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark11_RDD_Transform5 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("savsdfr")

    val sc: SparkContext = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // 抽样数据
    // 抽取不放回 : false, fraction:抽取的概率（0，1），seed：随机数种子
    // 抽取放回 ： true， fraction ：抽取的次数（1，2），seed：随机数种子

    val sampleRDD: RDD[Int] = numRDD.sample(true,2)

    sampleRDD.collect().foreach(println)

    sc.stop()
  }

}

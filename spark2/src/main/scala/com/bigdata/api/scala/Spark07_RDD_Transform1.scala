package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark07_RDD_Transform
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark07_RDD_Transform1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("setasda")

    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val newNumRdd: RDD[Int] = numRdd.map(num => {
      println("xxxxxxxxx")
      num * 2
    })
    newNumRdd.collect().foreach(println)
    sc.stop()
  }

}

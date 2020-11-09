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
object Spark12_RDD_Transform6 {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("helloa")

    val sc: SparkContext = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2,3,3,3,4,4))

    val distinctRDD: RDD[Int] = numRDD.distinct()

    val di: RDD[Int] = numRDD.distinct(3)

    distinctRDD.collect().foreach(println)

    sc.stop()
  }

}

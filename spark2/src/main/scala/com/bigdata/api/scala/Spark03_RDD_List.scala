package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark03_RDD_List
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark03_RDD_List {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("listrdd")

    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(List(1,2,3,4))

    value.map(_*2).foreach(println(_))

    sc.stop()
  }

}

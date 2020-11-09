package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark04_RDD_File
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark04_RDD_File {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FILErDD")

    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("data/input")

    val mapRdd: RDD[Array[String]] = value.map(_.split(""))
    mapRdd.collect().foreach(println(_))
  }

}

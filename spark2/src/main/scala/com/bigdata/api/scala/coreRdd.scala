package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ClassName: coreRdd
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object coreRdd {

  def main(args: Array[String]): Unit = {
    /* val spark: SparkSession = SparkSession.builder().appName("coreRddscala")
       .master("local[*]").getOrCreate()
     val context: SparkContext = new SparkContext()*/
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("my app")

    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(List("pandas","i like pandas"))

    val lines1: RDD[String] = sc.textFile("data/hello.csv")

    val numRdd: RDD[String] = lines.filter(line=>line.contains("hello"))


  }

}

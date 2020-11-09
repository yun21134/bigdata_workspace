package com.bigdata.api.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark02_WordCount1
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark02_WordCount1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcout2")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("data/input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .collect().foreach(println(_))
  }

}

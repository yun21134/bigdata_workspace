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
object Spark09_RDD_Transform3 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gujoaidj1")

    val sc: SparkContext = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10,11,12,13,14),2)

    val indexRDD: RDD[(Int, Int)] = numRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map(data => {
        (index, data)
      }
      )
    })
    indexRDD.collect().foreach(println)
    sc.stop()
  }

}

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
object Spark13_RDD_Transform7 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dfgsda").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(1 to 16 ,4 )

    val colesceRDD: RDD[Int] = rdd.coalesce(6)

    colesceRDD.glom().collect().foreach(list=>{
      println("***********")
      list.foreach(println)
    })

    val value: RDD[Int] = rdd.repartition(3)

    sc.stop()

  }

}

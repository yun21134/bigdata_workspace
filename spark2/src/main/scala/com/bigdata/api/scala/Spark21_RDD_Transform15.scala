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
object Spark21_RDD_Transform15 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    val rdd1: RDD[(Int, String)] = rdd.sortByKey()

    rdd1.collect().foreach(println)

    val rdd3: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    rdd3.mapValues(s=>s+"|||").collect().foreach(println)



  }

}

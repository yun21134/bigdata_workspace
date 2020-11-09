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
object Spark17_RDD_Transform11 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform11").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b",2), ("a",3), ("b",4)))

    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    val rdd2: RDD[(String, Int)] = rdd1.map {
      case (datas1, datas) => {
        (datas1, datas.sum)
      }
    }
    rdd2.collect().foreach(println)

    sc.stop()
  }

}

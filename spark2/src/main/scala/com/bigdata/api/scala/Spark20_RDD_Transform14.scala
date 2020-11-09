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
object Spark20_RDD_Transform14 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(
      Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
      2
    )

    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey[(Int, Int)](
      (num: Int) => (num, 1),
      (t: (Int, Int), num: Int) => {
        (t._1 + num, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)

      }
    )

  }

}

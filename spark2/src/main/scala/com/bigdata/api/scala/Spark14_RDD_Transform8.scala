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
object Spark14_RDD_Transform8 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("appnasd")

    val sc: SparkContext = new SparkContext(conf)

    // 转换算子 - union - 并集
    // 转换算子 - subtract - 差集
    // 转换算子 - intersection - 交集
    // 转换算子 - cartesian - 笛卡尔乘集
    // 转换算子 - zip - 拉链
    //       Can only zip RDDs with same number of elements in each partition
    //       Can't zip RDDs with unequal numbers of partitions

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7,8,9),3)

    val rdd3: RDD[Int] = rdd1.union(rdd2)

    val rdd4: RDD[Int] = rdd2.subtract(rdd1)

    val rdd5: RDD[Int] = rdd1.intersection(rdd2)

    val rdd6: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    val rdd7: RDD[(Int, Int)] = rdd1.zip(rdd2)

    rdd7.collect().foreach(println)

    sc.stop()

  }

}

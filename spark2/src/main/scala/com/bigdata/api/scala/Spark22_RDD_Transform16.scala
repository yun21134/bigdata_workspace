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
object Spark22_RDD_Transform16 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    // 转换算子 - join
    // (Int, String)
    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.makeRDD(List((1, "aa"), (2, "bb"), (4, "cc")))

    val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)

    rdd3.collect().foreach(println)

    val rdd4: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)


    rdd4.collect().foreach(println)

    sc.stop()
  }


}

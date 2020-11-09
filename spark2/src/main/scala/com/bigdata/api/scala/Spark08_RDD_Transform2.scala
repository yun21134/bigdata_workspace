package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark08_RDD_Transform2
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark08_RDD_Transform2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("confssdasd").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    numRdd.mapPartitions(datas=>{
      println("************")
      datas.map(_*2)
    }).collect().foreach(println)

      sc.stop()
  }

}

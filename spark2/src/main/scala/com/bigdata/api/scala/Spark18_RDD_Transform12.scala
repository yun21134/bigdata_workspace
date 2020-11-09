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
object Spark18_RDD_Transform12 {

  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b",2), ("a",3), ("b",4)))

    rdd.reduceByKey((x,y)=>{x+y}).collect().foreach(println)


    val value: RDD[(String, Int)] = rdd.groupByKey().map {
      case (datas1, datas) => {
        println("******************")
        (datas1, datas.sum)
      }
    }
    rdd.reduceByKey(_+_).collect().foreach(println(_))

    sc.stop()
  }


}

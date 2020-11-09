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
object Spark19_RDD_Transform13 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b",2), ("b",3), ("a",3), ("b",4), ("a", 5)), 2)

    // 取出每个分区相同key对应值的最大值，然后相加
    // ("a",1), ("b",2), ("b",3) => ("a",10), ("b",10)
    // ("a",3), ("b",4), ("a",5) => ("a",10), ("b",10)
    // (a, 20), (b, 20)
    // TODO aggregateByKey使用了函数柯里化
    // 存在两个参数列表
    // 第一个参数列表表示分区内计算时的初始值（零值）
    // 第二参数列表中需要传递两个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
    //val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)((x,y)=>{Math.max(x,y)}, (x,y)=>{x+y})
    //val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(10)((x,y)=>{Math.max(x,y)}, (x,y)=>{x+y})
    //val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_, _+_)

    rdd.aggregateByKey(0)((x,y)=>{Math.max(x,y)}, (x,y)=>{x+y})




  }


}

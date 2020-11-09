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
object Spark10_RDD_Transform4 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("asdcxv").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    /*val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)))

    val numRDD: RDD[Int] = listRDD.flatMap(list=>list)

    numRDD.collect().foreach(println)*/

    /*val numRDD: RDD[Int] = sc.makeRDD(List(7,4,3,2,5,4,8),2)

    val arrayRDD: RDD[Array[Int]] = numRDD.glom()

    arrayRDD.collect().foreach(datas=>{
      println("*******")
      println(datas.max)
    })*/

    /*val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val groupRDD: RDD[(Int, Iterable[Int])] = numRDD.groupBy(num=>num%2)

    groupRDD.collect().foreach(println)*/

    val stringRDD: RDD[String] = sc.makeRDD(List("xiaoming","xiaojiang","xiaohe","dazhi"))

    val value: RDD[String] = stringRDD.filter(s=>s.contains("xiao"))

  value.collect().foreach(println)

  sc.stop()
}

}

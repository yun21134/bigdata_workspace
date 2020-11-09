package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: spark01_WordCount
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object spark01_WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("data/input")

    val worRdd: RDD[String] = lineRDD.flatMap(line=>line.split(" "))

    val wordToOneRDD: RDD[(String, Int)] = worRdd.map((_,1))

    val wordToSumRdd: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_+_)

    val value: RDD[(String, Iterable[Int])] = wordToSumRdd.groupByKey()

    val results: Array[(String, Iterable[Int])] = value.collect()

    results.foreach(println(_))

    sc.stop()
  }
  
}

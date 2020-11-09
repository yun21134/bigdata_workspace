package com.lpy.big.data

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, rdd}

/**
  *
  * @ClassName: test2
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object test1 {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("my app")
  val sc: SparkContext = new SparkContext(conf)
  //val input: RDD[String] = sc.textFile("hello.txt")
  def main(args: Array[String]): Unit = {
    test2()
  }


  def test3(): Unit = {

    // 假设相邻页面列表以Spark objectFile的形式存储
    val links: RDD[(String, Seq[String])] = sc.objectFile[(String, Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()
    // 将每个页面的排序值初始化为1.0；由于使用mapValues，生成的RDD
    // 的分区方式会和"links"的一样
    val ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)
    // 运行10轮PageRank迭代
    for(i <- 0 until 10){
      val contributions: RDD[(String, Double)] = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      contributions.reduceByKey((x,y)=>x+y).mapValues(v => 0.15 + 0.85*v)
    }
    ranks.saveAsTextFile("ranks")

  }

  def test2(): Unit = {

    val rdd = sc.parallelize(List(2,3,4,5))

    rdd.flatMap(x=>x.to(3)).collect().foreach(x=>print(x))

   /* val result: RDD[Integer] = input.map(x =>  x + 1)

    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(","))*/

  }

  def test1(args: Array[String]): Unit = {

    val lines: RDD[String] = sc.parallelize(List("hello word" , "hi"))

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    println(words.first())
  }

}

package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark23_RDD_Demo
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark23_RDD_Demo {

  def main(args: Array[String]): Unit = {
    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    // TODO 需求：统计出每一个省份广告被点击次数的TOP3

    // TODO 1. 从日志中获取用户的广告点击数据
    val lineRDD: RDD[String] = sc.textFile("data/agent.log")

    // TODO 2. 将数据转换结构 : (prv - adv, 1)
    val mapRDD: RDD[(String, Int)] = lineRDD.map(line => {
      //切分数据
      val datas: Array[String] = line.split(" ")
      (datas(1) + "_" + datas(4), 1)
    })

    // TODO 3. 将转换结构后的数据进行聚合统计 :  (prv - adv, 1) => (prv - adv, sum)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    // TODO 4. 将聚合的结果进行机构的转换 ： (prv - adv, sum) => ( prv, (adv, sum) )
    val prvToAdvAndSumRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0), (keys(1), sum))
      }
    }
    // TODO 5. 将转换结构后的数据按照省份进行分组 ： ( prv, Iterator[(adv, sum)] )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvToAdvAndSumRDD.groupByKey()

    // TODO 6. 将分组后的数据进行排序(降序)，取前3名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })

    // TODO 7. 将结果打印到控制台
    resultRDD.collect().foreach(println)
    println("计算朝堂,诈死脱身,三千千无水")
    sc.stop()
  }

}

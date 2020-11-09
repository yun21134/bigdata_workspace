package com.bigdata.api.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  *
  * @ClassName: Spark15_RDD_Transform9
  * @Author: lipy
  * @Date:
  * @Version : V1.0
  *
  **/
object Spark16_RDD_Transform10 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,1),(3,1),(4,1)))

    val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new MyPartitioner(3))

    val rdd3: RDD[(Int, (Int, String))] = rdd2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    rdd3.collect().foreach(println)

    sc.stop()
  }

}

class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = 2
}

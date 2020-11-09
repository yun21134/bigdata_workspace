package com.bigdata.api.workspace;

import com.bigdata.api.scala.Flight;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: coreRdd
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/
public class coreRdd {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("dataset actions")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String[] splits = new String("Spark The Definitive Guide : Big Data"  +
                "Processing Made Simple").split(" ");
        JavaRDD<String> words = sc.parallelize(Arrays.asList(splits), 2);

        Map<String,Long> supplementalData = new HashMap();
        supplementalData.put("Spark",1000L);
        supplementalData.put("Definitive",200L);
        supplementalData.put("Big",-300L);
        supplementalData.put("Simple",100L);

        Broadcast<Map<String, Long>> suppBroadcast = sc.broadcast(supplementalData);

        /*suppBroadcast.value().entrySet()
                .forEach(e-> System.out.println(e.getKey()+" : "+e.getValue()));*/

        words.map(word -> new Tuple2(word,suppBroadcast.value().getOrDefault(word,0L)))
                .sortBy(pair -> pair._2,true,2)
                .collect().forEach(t -> System.out.println(t._1+" ->"+t._2));


        Dataset<Flight> flights = spark.read().parquet("data/fight.parquet").as(Encoders.bean(Flight.class));

        flights.printSchema();
        /*flights.where("ORIGIN_COUNTRY_NAME =  'China' or" +
                "DEST_COUNTRY_NAME = " +
                "'China'").select(sum("count"));*/

    }

}

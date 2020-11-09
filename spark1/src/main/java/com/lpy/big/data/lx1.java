package com.lpy.big.data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @ClassName: test1
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/
public class lx1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("hh");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("hello.txt");


        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                }
        );
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2<>(x, 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        counts.saveAsTextFile("D:\\Demo\\demo\\output");
    }

}
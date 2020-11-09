package com.lpy.big.data;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.uncommons.maths.statistics.DataSet;
import org.apache.spark.sql.functions;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @ClassName: lx2
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/
public class lx2 {

    public static void main(String[] args) {

    }

    public static void structured(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("hh");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext spark = new HiveContext(sc);

        Row row = RowFactory.create("hello", null, 1, false);
        /*System.out.println(row.get(0));
        System.out.println(row.getString(0));
        System.out.println(row.getInt(2));
        System.out.println(row.getBoolean(3));*/

        ArrayList<StructField> fields3 = new ArrayList<>();
        fields3.add(DataTypes.createStructField("c1",DataTypes.StringType,true));
        fields3.add(DataTypes.createStructField("c2",DataTypes.StringType,true));
        fields3.add(DataTypes.createStructField("c3",DataTypes.IntegerType,true));
        fields3.add(DataTypes.createStructField("c4",DataTypes.BooleanType,true));
        StructType structType3 = DataTypes.createStructType(fields3);


        DataFrame df4 = spark.createDataFrame(Arrays.asList(row), structType3);
        /*df4.printSchema();
        df4.show();*/

        /*df4.select(functions.expr("c3 +1").alias("c3plus1")).show();
        df4.selectExpr("c3 + 1 as c3plus1").show();*/
        /*df4.withColumn("c3x10",functions.expr("c3 * 10"))
                .withColumn("one",functions.lit(1))
                .selectExpr("c3x10","one","c3x10 - one as res")
                .show();*/
        /*df4.drop("").columns();*/

        /*df4.filter("count").show();*/

    }

    public static void wordcount() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("hh");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        System.out.println(words.first());
    }
}

package com.bigdata.api.workspace;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
/**
 * @ClassName: lx1
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/
public class lx1 {

    public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                    .appName("dataset actions")
                    .master("local[*]")
                    .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/hello.csv");

        //df.printSchema();
        //df.createOrReplaceTempView("tfTable");
        //df.select(lit(5).alias("col_5"),lit("five"),
                //lit(5.0)).show(1);

        df.where(col("InvoiceNo").equalTo("536365"))
                .select("InvoiceNo","Description")
                .show(3,false);
        df.where("InvoiceNo = 536365").show(3,false);

       /* df.where(col("InvoiceNo").equalTo(536365))
                .select("InvoiceNo","Description")
                .show(5,false);

        df.where(col("InvoiceNo") ===536365)
                .select("InvoiceNo","Description")
                .show(5,false);

        df.where("InvoiceNo = 536365")
                .show(6,false);*/

        Column fabricatedQuantity = pow(col("Quantity").multiply(col("UnitPrice")), 2).plus(5);
        df.select(expr("CustomerId"),
                fabricatedQuantity.alias("realQuantity")).show(2);
        df.selectExpr("CustomerId","POWER(Quantity * UnitPrice,2)+5) as realQuantity").show(2);




    }

    public static void BasicActions(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("dataset actions")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/hello.csv");

        //df.printSchema();
        //df.createOrReplaceTempView("tfTable");

        //df.select(lit(5).alias("col_5"),lit("five").alias("hello"),
        //        lit(5.0)).show(1);

        df.where(col("InvoiceNo").equalTo("536365"))
                .select("InvoiceNo","Description").show(3,false);
        df.where("InvoiceNo = 536365").show(3,false);




    }

}

package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.spark.sql.functions
import org.apache.spark.sql.Encoders

object P2 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Performance")
      .getOrCreate()
    import spark.implicits._

    val inputfile = spark.read.parquet("hdfs://ch:8020/user/data/modelled/OTP-1518688000000/*snappy.parquet")
    .filter($"DepTime" !== "NA").filter($"DepDelay" !== "NA")
    .select($"DepTime", $"DepDelay").as[(String,String)]
    val df = inputfile.map(line =>  line._1.trim().length() match {
        case 1 => ("0", line._2)
        case 2 => ("0", line._2)
        case 3 => (line._1.substring(0, 1), line._2)
        case 4 => (line._1.substring(0, 2), line._2)
    }).toDF("Deptime", "Depdelay")
    
    val result = df.withColumn("Depdelay", col("Depdelay").cast("Double"))
      .groupBy("Deptime")
      .agg(avg("Depdelay").as("result"))
      println("\nThe best time in the day with min delay is  "+(result.orderBy(asc("result")).take(1) mkString).split(",")(0).stripPrefix("[")+" o'clock")
      
  }
}
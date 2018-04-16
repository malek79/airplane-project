package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Encoders
object P3 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Performance")
      .getOrCreate()
    import spark.implicits._
  
    val planedata = spark.read.parquet("hdfs://ch:8020/user/data/modelled/Planedata/*snappy.parquet")
    val otpdata = spark.read.parquet("hdfs://ch:8020/user/data/modelled/OTP-1518688000000/*snappy.parquet")

    val joined_df = planedata.join(
      otpdata, planedata.col("tailnum") === otpdata.col("tailnum"), "inner")
      .select($"issue_date", $"ArrDelay").as[(String, String)]
      .map(line => (line._1.split("/").takeRight(1) mkString, line._2)).toDF("issue_date", "ArrDelay")

    joined_df.withColumn("ArrDelay", col("ArrDelay").cast("Double"))
      .groupBy("issue_date")
      .agg(avg("ArrDelay").as("AVG_DealyPerYear")).filter($"issue_date" !== "None").orderBy(asc("issue_date")).show(100)
  }
}
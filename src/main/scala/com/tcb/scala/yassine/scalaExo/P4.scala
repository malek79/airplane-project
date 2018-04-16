package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Encoders
object P4 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Performance")
      .getOrCreate()
    import spark.implicits._
    println("\n")

    val otpdata = spark.read.parquet("hdfs://ch:8020/user/data/modelled/OTP-1518688000000/*snappy.parquet").filter($"DepDelay" > 0)
    otpdata.show()
    val otp2 = otpdata.groupBy($"Origin", $"Dest")
      .agg(avg("DepDelay").as("DepDelay"), avg("ArrDelay").as("ArrDelay")).toDF()

    val destotp = otp2.groupBy("Dest").agg(avg("DepDelay").as("DepDelay"), avg("ArrDelay").as("first_ArrDelay")).toDF()
    val originotp = otp2.groupBy("Origin").agg(avg("DepDelay").as("DepDelay_(after_arrival)"), avg("ArrDelay").as("ArrDelay_(final)")).toDF()

    val joined_df = destotp.join(
      originotp, destotp.col("Dest") === originotp.col("Origin"), "cross")
      
      joined_df.show()
      
  }
}
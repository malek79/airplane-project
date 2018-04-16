package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.spark.sql.functions

object Performance {
  implicit class OpsNum(val str: String) extends AnyVal {
    def isNumeric() = Try(str.toDouble).isSuccess
  }
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Performance")
      .getOrCreate()
    import spark.implicits._

    val inputfile = spark.read.parquet("hdfs://ch:8020/user/data/modelled/OTP-1518688000000/*snappy.parquet")

    //    inputfile.groupBy("UniqueCarrier").agg(count(when($"UniqueCarrier" === "UA", 1)).as("UA_count"),count(when($"UniqueCarrier" === "WN", 1)).as("WN_count")).show
    inputfile.groupBy("UniqueCarrier").count().show()
//    println(inputfile.groupBy("UniqueCarrier").count().orderBy(desc("count")).take(1) mkString "\n")//.foreach(print))//agg(max("count")).show()
    var max_carrier = (inputfile.groupBy("UniqueCarrier").count().orderBy(desc("count")).take(1) mkString).split(",")(0).stripPrefix("[")
    
    val res = inputfile.withColumn("ArrDelay", col("ArrDelay").cast("Double"))
      .groupBy("UniqueCarrier")
      .agg(avg("ArrDelay").as("res"))

    res.show()
//    res.agg(max("res")).show()
    var avg_carrier:String = (res.orderBy(asc("res")).take(1) mkString).split(",")(0).stripPrefix("[").trim()
    println("carrier ID : "+avg_carrier)
    
    val carriersFile = spark.read.csv("hdfs://ch:8020/user/data/raw/Carriers-1518687660000/part*")
    carriersFile.foreach(line =>if(line.toString().split(",")(0).stripPrefix("[").stripSuffix("]").trim().equals(avg_carrier)) 
                                  println("Using the average ArrDelay : "+line.toString().split(",")(1).stripPrefix("\"").stripSuffix("]"))
                                  else
                                  if(line.toString().split(",")(0).stripPrefix("[").stripSuffix("]").trim().equals(max_carrier))
                                      println("Using max trips : "+line.toString().split(",")(1).stripPrefix("\"").stripSuffix("]"))
    )
//    println(res.select(max(struct( $"res" +: res.columns.collect {case x if x!= "res" => col(x)}: _*))).first mkString "\n")
//    res.select(functions.max("min(ArrDelay)").as("max")).show()
    //    inputfile.foreach(f => if (f.toString().split(",").length == 27 && f.toString().split(",")(15).isNumeric()) {
    ////        println(f.toString().split(",")(15).toInt)
    //        sum += (f.toString().split(",")(15).toDouble/moy)
    //        println("the avg is "+sum) //200931   1194867
    //    })
  }
}
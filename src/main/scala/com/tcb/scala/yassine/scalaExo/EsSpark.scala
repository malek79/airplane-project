package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
object EsSpark {
  case class carriers(code: String, description: String)
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(EsSpark.getClass.getName)
    val spark = SparkSession.builder().master("local[*]").config(conf).getOrCreate()
    import spark.sqlContext.implicits._

    // Elastic connection parameters
    val elasticConf: Map[String, String] = Map("es.nodes" -> "localhost",
      "es.clustername" -> "elasticsearch")//"es.mapping.id" -> "id"

    val indexName = "otp_index"
    val mappingName = "otp_index_type"
    
//    val data = spark.read.option("header", "true").csv("hdfs://ch:8020/user/data/raw/Airport/AirportsData.1519306291434.txt")
    val data = spark.read.parquet("hdfs://ch:8020/user/data/modelled/OTP-1518688000000/part*")
    println(data.count())

//    val usersFile = spark.sparkContext.textFile("hdfs://ch:8020/user/data/raw/Carriers-1518687660000/part*")
//    val userHeader = usersFile.first()
//    val userRecords = usersFile.filter(x => x != userHeader)
//    val usersDF = userRecords.map(x => x.split(",", -1)).map(u => carriers(u(0), u(1))).toDF("code", "description")
    // Dummy DataFrame
//    val df = spark.sparkContext.parallelize((1 to 100).map(x => ExampleClass(x, s"${x}@gmail.com"))).toDF()

    // Write elasticsearch
    data.saveToEs(s"${indexName}/${mappingName}", elasticConf)


    // terminate spark context
    spark.stop()
  }
}
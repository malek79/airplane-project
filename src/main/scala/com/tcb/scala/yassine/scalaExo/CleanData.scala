package com.tcb.scala.yassine.scalaExo
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import java.util.UUID.randomUUID
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.net.URI
import scala.collection.Map

object CleanData {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("CleaningData")
      .getOrCreate()

    import spark.implicits._
    
    /* In case of an array input*/
    val paths = Array("/user/data/raw/Planedata/") //"/user/data/raw/OTP-1518688000000/", "/user/data/raw/Airport/", "/user/data/raw/Planedata/",
    //      "/user/data/raw/Carriers-1518687660000/")

    for (p <- paths) {
      var folder = spark.sparkContext.wholeTextFiles(p)
      val conf = new Configuration()
      val fs = FileSystem.get(new URI("hdfs://ch:8020"), conf)
      var filePath = new Path(p)
      val status = fs.listStatus(filePath)
      def modifyFile(file: String) = {
        if (file.contains("_SUCCESS")) { //== "_SUCCESS") {
          println("Will Not Process '_SUCCESS' File")
        } else {
          var filename: String = file.split("/").takeRight(1).toList mkString "\n";
          var pathname: String = p.split("/").takeRight(1).toList mkString "\n"
          /*
         * logic of processing a single file
         * */
          val logData = spark.read.option("header", "true").csv(file)
          /* For Filtering the Plane Data */
          val linedata = logData.filter(f => (f != null) && (!f.isNullAt(3)))

          /* General Filter Options
          //          val linedata = logData.map(line => line.toString().replaceAll("\\bNA\\w*", "0").replaceAll("\\b,,\\w*", ",NULL,"))
          */
          
          /*  Filtering the OTP data 
          val df = logData.toDF("Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier",
            "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance",
            "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

          val newdf = df.withColumn("LateAircraftDelay", when(col("LateAircraftDelay") === "NA", "0").otherwise(col("LateAircraftDelay")))
                        .withColumn("SecurityDelay", when(col("SecurityDelay") === "NA", "0").otherwise(col("SecurityDelay")))
                        .withColumn("NASDelay", when(col("NASDelay") === "NA", "0").otherwise(col("NASDelay")))
                        .withColumn("WeatherDelay", when(col("WeatherDelay") === "NA", "0").otherwise(col("WeatherDelay")))
                        .withColumn("CarrierDelay", when(col("CarrierDelay") === "NA", "0").otherwise(col("CarrierDelay")))
                        .withColumn("DepTime", when(col("DepTime") === "NA", "0").otherwise(col("DepTime")))
                        .withColumn("CarrierDelay", when(col("CarrierDelay") === "NA", "0").otherwise(col("CarrierDelay")))
          newdf.show()
          //BEST METHOD
           * linedata.na.replace("colnames", <map of replacement exo: val states = Map("AL" -> "Alabama", "AK" -> "Alaska") >)
          
          */

          /*  Other method
          val res = udf { (LateAircraftDelay: String) =>
            if (LateAircraftDelay == "NA") "0" else LateAircraftDelay
          }
          df.withColumn("LateAircraftDelay", res(df("LateAircraftDelay"))).show

          */
          // val newdf = linedata.toDF()

          linedata.write.mode(SaveMode.Overwrite).parquet("hdfs://ch:8020/user/data/modelled/" + pathname)

        }
      }

      status.foreach(filename => modifyFile(filename.getPath.toString()))
    } //for loop end
  }
}
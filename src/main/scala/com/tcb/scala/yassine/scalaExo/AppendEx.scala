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

object AppendEx {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    val paths = Array("/user/data/raw/OTP-1518688000000/", "/user/data/raw/Airport/", "/user/data/raw/Planedata/",
      "/user/data/raw/Carriers-1518687660000/")

    for (p <- paths) {
//      var folder = spark.sparkContext.wholeTextFiles(p)
      val conf = new Configuration()
      val fs = FileSystem.get(new URI("hdfs://ch:8020"), conf)
      var filePath = new Path(p)
      val status = fs.listStatus(filePath)
      //      var files = folder.map { case (filename, content) => filename }
      def modifyFile(file: String) = {
        if (file.contains("_SUCCESS")) { //== "_SUCCESS") {
          println("Will Not Process '_SUCCESS' File")
        } else {
          var filename: String = file.split("/").takeRight(1).toList mkString "\n";
          var pathname: String = p.split("/").takeRight(1).toList mkString "\n"
          /*
         * logic of processing a single file
         * */
          val logData = spark.sparkContext.textFile(file);
          val linedata = logData.map(line => line.concat("," + System.currentTimeMillis / 1000) + "," + randomUUID().toString);
          // linedata.saveAsTextFile("hdfs://ch:8020/user/data/test/output/" + name)

          val df = linedata.toDF();
          //        df.write.mode(SaveMode.Overwrite).text("hdfs://ch:8020/user/data/test/output/" + name)
          df.write.avro("hdfs://ch:8020/user/data/decomposed/" + pathname + "/" + filename)

        }
      }

      status.foreach(filename => modifyFile(filename.getPath.toString()))
      //      files.map(filename => {
      //        modifyFile(filename)
      //      })
    } //for loop end
  }
}
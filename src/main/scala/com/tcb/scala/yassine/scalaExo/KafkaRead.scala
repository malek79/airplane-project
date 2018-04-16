package com.tcb.scala.yassine.scalaExo

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
/**
 * Consumes messages from one or more topics in Kafka and writes the content to hdfs folders.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers, not used in this code
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * For any rdd convertion to df inside of foreachRDD
 * val conf = new SparkConf().setMaster("local").setAppName("My App")
 * val sc = new SparkContext(conf)
 * val sqlContext = new SQLContext(sc) 
 * import sqlContext.implicits._
 */
object KafkaRead {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
					|Usage: must contain two arguments
					|  <brokers> is a list of one or more Kafka brokers
					|  <topics> is a list of one or more kafka topics to consume from
					|
					""".stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args
    
    // Create context with 60 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaLoad").setMaster("local[*]")
//    sparkConf.set("dfs.blocksize", "1024 * 1024 * 16")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ch:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group14",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "kafka.bootstrap.servers" -> "ch:6667")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    messages.foreachRDD(rdd => {
      val pairRdd = rdd.map(i => (i.topic(), i.value()))
      
      for (topic <- topicsSet) {
        val topicRdd = pairRdd.map(f => if (f._1.equals(topic)) f._2).filter(f => (f != null) && (f.toString().length > 10))

        if (topicRdd.count() > 0) //isempty()
          topicRdd.saveAsTextFile("hdfs://ch:8020/user/data/test2/" + topic)

      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
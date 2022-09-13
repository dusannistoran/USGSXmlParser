package com.example

import com.example.Utils.extractIdsFromEvents
import com.example.Utils.getHourAgoFormattedTime
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object App {
  
  def main(args: Array[String]): Unit = {

    val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
    val sparkCores: String = configSpark.getString("master")
    val checkpoint: String = configSpark.getString("checkpointLocation")

    lazy val spark = SparkSession
      .builder()
      .config("spark.speculation", "false")
      .config("checkpointLocation", s"$checkpoint")
      .master(s"$sparkCores")
      .appName("produce to Kafka xml from local file")
      .getOrCreate()

    LoggerFactory.getLogger(spark.getClass)
    spark.sparkContext.setLogLevel("WARN")

    val hourAgoFormatted = getHourAgoFormattedTime()
    println("hour ago UTC: " + hourAgoFormatted)

    val url = s"https://earthquake.usgs.gov/fdsnws/event/1/query?format=xml&starttime=$hourAgoFormatted"

    val xmlParser = new XMLParser
    val earthquakes: List[Earthquake] = xmlParser.parseXMLToQuakesObjects(url)

    val sc = spark.sparkContext
    val rddData = sc.parallelize(earthquakes)
    val rddDataArray = rddData.collect()
    println("There are " + rddDataArray.size + " rdds")
    rddDataArray.foreach(e => {
      println("id: " + e.id)
      println("time: " + e.time)
      println("magnitude: " + e.mag)
      println("location: " + e.location)
      println("longitude: " + e.longitude)
      println("latitude: " + e.latitude)
      println("depth: " + e.depth)
      println("\n")
    })

    import spark.implicits._
    val dfData = rddData.toDF()
    println("dfData schema:")
    dfData.printSchema()

    import org.apache.spark.sql.functions._
    dfData.select(to_json(struct("*")).alias("value"))

    dfData.selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "earthquakes-xml")
      .save()

  }

  case class Earthquake(
                         id: String,
                         time: String,
                         mag: Double,
                         location: String,
                         longitude: Double,
                         latitude: Double,
                         depth: Int
                       )

}

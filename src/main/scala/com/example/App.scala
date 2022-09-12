package com.example

import com.example.Utils.extractIdsFromEvents
import com.example.Utils.getHourAgoFormattedTime
import com.example.Utils.getKafkaProducerProps
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.databricks.spark.xml._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.time._
import java.time.format.DateTimeFormatter
import requests._

import scala.xml._
//import upickle._

object App {



  def main(args: Array[String]): Unit = {

    val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
    val sparkCores: String = configSpark.getString("master")
    val checkpoint: String = configSpark.getString("checkpointLocation")
    //val xmlFileLocation: String = configSpark.getString("xmlFileLocation")

    lazy val spark = SparkSession
      .builder()
      .config("spark.speculation", "false")
      .config("checkpointLocation", s"$checkpoint")
      .master(s"$sparkCores")
      .appName("produce to Kafka xml from local file")
      .getOrCreate()

    LoggerFactory.getLogger(spark.getClass)
    spark.sparkContext.setLogLevel("WARN")

    //val utcZoneId = ZoneId.of("UTC")
    //val zonedDateTime = ZonedDateTime.now
    //val utcDateTime = zonedDateTime.withZoneSameInstant(utcZoneId)
    //val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    //println("utc now: " + utcDateTime.format(formatter))
    //val hourAgo = utcDateTime.minusHours(1)
    //val hourAgoFormatted = hourAgo.format(formatter)
    val hourAgoFormatted = getHourAgoFormattedTime()
    //val tenMinutesAgo = utcDateTime.minusMinutes(10)
    //val tenMinutesAgoFormatted = tenMinutesAgo.format(formatter)
    println("hour ago UTC: " + hourAgoFormatted)

    val url = s"https://earthquake.usgs.gov/fdsnws/event/1/query?format=xml&starttime=$hourAgoFormatted"
    //val response = requests.get(url)
    //println("response status code: " + response.statusCode)
    //val responseText = response.text
    //println("response text:")
    //println(responseText)

    //val elem = scala.xml.XML.loadString(responseText)
    //println("elem label: " + elem.label)

    //val elemText = elem.text
    //println("elemText:")
    //println(elemText)

    //val events = elem \\ "event"
    //println("THERE ARE " + events.size + " EVENTS")

    //println("TEST!!!")
    //events.foreach(e => {
    //  val time = e \\ "time"
    //  val timeValue = time \ "value"
    //  println(timeValue.text)
    //})

    //val ids: List[String] = extractIdsFromEvents(events)
    //println("ids:")
    //ids.foreach(println)
    //val times = elem \\ "time"
    //val timeValues = times \ "value"
    //println("timeValues:")
    //timeValues.foreach(v => println(v.text))
    //val magnitudes = elem \\ "mag"
    //val magnitudeValues = magnitudes \ "value"
    //println("magnitudeValues:")
    //magnitudeValues.foreach(v => println(v.text))
    //val places = elem \\ "text"
    //places.foreach(p => println(p.text))
    //val longitudes = elem \\ "longitude"
    //val longitudeValues = longitudes \ "value"
    //println("longitudeValues:")
    //longitudeValues.foreach(v => println(v.text))
    //val latitudes = elem \\ "latitude"
    //val latitudeValues = latitudes \ "value"
    //println("latitudeValues:")
    //latitudeValues.foreach(v => println(v.text))
    //val depths = elem \\ "depth"
    //val depthValues = depths \ "value"
    //println("depthValues:")
    //depthValues.foreach(v => println(v.text))

    val xmlParser = new XMLParser
    val earthquakes: List[Earthquake] = xmlParser.parseXMLToQuakesObjects(url)
    /*
    earthquakes.foreach(e => {
      println((earthquakes.indexOf(e) + 1) + ". ")
      println("id: " + e.id)
      println("time: " + e.time)
      println("mag: " + e.mag)
      println("location: " + e.location)
      println("longitude: " + e.longitude)
      println("latitude: " + e.latitude)
      println("depth: " + e.depth)
      println("\n")
    })
     */

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
      //.outputMode("append")
      //.option("checkpointLocation", "/home/dusan/scalaProjects/USGSXmlParser/src/main/resources")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "earthquakes-xml")
      .save()


    //val events: scala.xml.NodeSeq = elem \ "quakeml" \ "eventParameters" \ "event"
    //println("eventParameters label: " + eventParameters.l)
    //println("events:")
    //events.foreach(println)




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

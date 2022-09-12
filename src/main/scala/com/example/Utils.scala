package com.example

import org.apache.kafka.common.serialization.StringSerializer

import java.time._
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.xml.NodeSeq
import scala.xml.Node

object Utils {

  def getHourAgoFormattedTime(): String = {
    val utcZoneId = ZoneId.of("UTC")
    val zonedDateTime = ZonedDateTime.now
    val utcDateTime = zonedDateTime.withZoneSameInstant(utcZoneId)
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    println("utc now: " + utcDateTime.format(formatter))
    val hourAgo = utcDateTime.minusHours(1)
    val hourAgoFormatted = hourAgo.format(formatter)
    hourAgoFormatted
  }

  def extractIdsFromEvents(events: NodeSeq): List[String] = {
    val eventsList = events.toList
    val publicIdsList: List[String] = eventsList.map(event => event.attribute("publicID").getOrElse().toString)
    val ids = publicIdsList.map(ev => ev.substring(ev.indexOf("?eventid=") + 9, ev.indexOf("&amp")))
    ids
  }

  def extractIdFromEvent(event: Node): String = {
    val publicId = event.attribute("publicID").getOrElse().toString
    val id = publicId.substring(publicId.indexOf("?eventid=") + 9, publicId.indexOf("&amp"))
    id
  }

  def getKafkaProducerProps(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

}

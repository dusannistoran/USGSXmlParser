package com.example

import com.example.Utils.{extractIdFromEvent, extractIdsFromEvents}
import com.example.App.Earthquake

import java.time.ZonedDateTime
import scala.collection.mutable.ListBuffer

class XMLParser {


  def parseXMLToQuakesObjects(url: String):List[Earthquake] = {
    val response = requests.get(url)
    if (response.statusCode == 200) {
      val responseText = response.text
      val elem = scala.xml.XML.loadString(responseText)

      val events = elem \\ "event"
      //val ids: List[String] = extractIdsFromEvents(events)

      val earthquakes: ListBuffer[Earthquake] = ListBuffer[Earthquake]()

      for(event <- events) {
        val id = extractIdFromEvent(event)
        val time = event \\ "time"
        val timeValue = time \ "value"
        val magnitude = event \\ "mag"
        val magnitudeValue = magnitude \ "value"
        val place = event \\ "text"
        val longitude = event \\ "longitude"
        val longitudeValue = longitude \ "value"
        val latitude = event \\ "latitude"
        val latitudeValue = latitude \ "value"
        val depth = event \\ "depth"
        val depthValue = depth \ "value"
        val earthquake = Earthquake(
          id,
          timeValue.text,
          Math.rint(magnitudeValue.text.toDouble * 10) / 10,
          place.text,
          longitudeValue.text.toDouble,
          latitudeValue.text.toDouble,
          Math.rint(depthValue.text.toDouble).toInt
        )
        earthquakes += earthquake
      }
      earthquakes.toList
    }
    else throw new Exception("Reponse status code is not equal 200")
  }

}

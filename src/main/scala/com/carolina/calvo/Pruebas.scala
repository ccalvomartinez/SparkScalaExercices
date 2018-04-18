package com.carolina.calvo


import scalaj.http.Http

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object Pruebas extends App {
//, ("key", "AIzaSyC2IazqIMaI87xmoNWz07iTnJouEa4Ycfw")
  try {
    val response = Http("https://maps.googleapis.com/maps/api/geocode/json")
      .params(("latlng", "40.714224,-73.961452"), ("result_type", "country"), ("key", "AIzaSyC2IazqIMaI87xmoNWz07iTnJouEa4Ycfw")).asString
    val parsedBody = JSON.parseRaw(response.body)
    parsedBody match {
      case Some(m: JSONObject) => {
        val results = m.obj("results").asInstanceOf[JSONArray]
        if(results.list.nonEmpty) {
          val countryData = results.list.head.asInstanceOf[JSONObject]
          val addresComponents = countryData.obj("address_components").asInstanceOf[JSONArray].list.head.asInstanceOf[JSONObject]
          val country = addresComponents.obj("long_name").toString
          println(country)
        }
      }
    }

  } catch {
    case e: Exception => println(e.getLocalizedMessage)
  }

}

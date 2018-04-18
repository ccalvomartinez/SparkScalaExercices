package com.carolina.calvo.utils

import java.sql.Date
import java.text.SimpleDateFormat

import scalaj.http.Http

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object utlis {
  def getCategory(description: String): String = {
    description match {
      case "Shoping Mall" => "Hogar"
      case "Restaurant" => "Ocio"
      case "Cinema" => "Ocio"
      case "Sports" => "Ocio"
      case "car insurance" => "Seguros"
      case "home insurance" => "Seguros"
      case "rent" => "Hogar"
      case "leasing" => "Otros"
      case _ => "Otros"
    }
  }

  def parseToDate(s: String, format: String): java.sql.Date = {

    val formatter2 = new SimpleDateFormat(format)
    val  fecha = formatter2.parse(s)

    new Date(fecha.getTime)
  }

  def getCountry(latitude: Double, longitude: Double): String = {
    try {
      val response = Http("https://maps.googleapis.com/maps/api/geocode/json")
        .params(("latlng", s"$latitude,$longitude"), ("result_type", "country"), ("key", "AIzaSyC2IazqIMaI87xmoNWz07iTnJouEa4Ycfw")).asString

      val parsedBody = JSON.parseRaw(response.body)

      parsedBody match {
        case Some(m: JSONObject) =>
          val results = m.obj("results").asInstanceOf[JSONArray]
          if(results.list.nonEmpty) {
            val countryData = results.list.head.asInstanceOf[JSONObject]
            val addressComponents = countryData.obj("address_components").asInstanceOf[JSONArray].list.head.asInstanceOf[JSONObject]
            val country = addressComponents.obj("long_name").toString
            country
          } else {
            "N/A"
          }
        case _ => "N/A"
      }

    } catch {
      case e: Exception => "N/A"
    }
  }
}

package com.carolina.calvo.model

import java.sql.Date


case class Geolocation(latitude: Double, longitude: Double, city: String, var country: String)

case class Client(id: Long, name: String, ccc: Long)

case class Transaction(idClient: Long, date: Date, amount: Double, description: String, category: String, creditCardType: String, geolocation: Geolocation)
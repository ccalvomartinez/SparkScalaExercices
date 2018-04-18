package com.carolina.calvo.speedlayer.generator

import com.carolina.calvo.model.TransactionRecord
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Random

object TransactionGeneratorRandom {

  def generateRandomTransaction(): String = {
    var transaction = ""
    val initialDate = new DateTime(2009, 1, 1, 0, 0)
    val generatedDate = initialDate.plusMinutes(Random.nextInt(1000000))
    val formatter = DateTimeFormat.forPattern("M/d/yy h:mm")
    transaction = s"${generatedDate.toString(formatter)}," +
      s"Product${Random.nextInt(5)}," +
      s"${Random.nextInt(6000)}," +
      s"$getRandomPaymentType," +
      s"$getRandomName," +
      s"$getRandomCity," +
      s"${Random.nextInt(30)}," +
      s"${generatedDate.toString(formatter)}," +
      s"$getRandomLatitude," +
      s"$getRandomLongitude," +
      s"$getRandomDescription"

    transaction
  }

  def generateRandomTransactionRecord(): TransactionRecord= {

    val initialDate = new DateTime(2009, 1, 1, 0, 0)
    val generatedDate = initialDate.plusMinutes(Random.nextInt(1000000))
    val formatter = DateTimeFormat.forPattern("M/d/yy h:mm")
    val transactionRecord = new TransactionRecord(generatedDate,
      s"Product${Random.nextInt(5)}",
      Random.nextInt(6000),
      getRandomPaymentType,
      getRandomName,
      getRandomCity,
      Random.nextInt(30),
      generatedDate,
      getRandomLatitude.toDouble,
      getRandomLongitude.toDouble,
      getRandomDescription)

  transactionRecord
  }

  private def getRandomPaymentType: String = {
    val randomInt = Random.nextInt(3)

    randomInt match {
      case 0 => "Mastercard"
      case 1 => "Visa"
      case 2 => "Amex"
      case 3 => "Dinners"
      case _ => "Euro6000"
    }
  }

  private def getRandomName: String = {
    val randomInt = Random.nextInt(10)

    randomInt match {
      case 0 => "Carolina"
      case 1 => "Betina"
      case 2 => "Federica"
      case 3 => "Gouya"
      case 4 => "Fleur"
      case 5 => "Adam"
      case 6 => "Leanne"
      case 7 => "Georgia"
      case 8 => "Richard"
      case _ => Random.alphanumeric.take(randomInt).mkString.replace(",", "")
    }
  }

  private def getRandomCity: String = {
    val randomInt = Random.nextInt(9)

    randomInt match {
      case 0 => "London"
      case 1 => "Astoria"
      case 2 => "Ottawa"
      case 3 => "Eagle"
      case 4 => "New York"
      case 5 => "Eindhoven"
      case 6 => "Manchester"
      case 7 => "Madrid"
      case 8 => "Barcelona"
      case _ => Random.alphanumeric.take(randomInt).mkString.replace(",", "")
    }
  }

  private def getRandomLatitude: String = {
    val latitude = (Random.nextDouble()*171 - 85).toString
    latitude
  }

  private def getRandomLongitude: String = {
    (Random.nextDouble()*361 - 180).toString
  }

  private def getRandomDescription: String = {
    val randomInt = Random.nextInt(9)

    randomInt match {
      case 0 => "Shopping Mall"
      case 1 => "Restaurant"
      case 2 => "Cinema"
      case 3 => "Sports"
      case 4 => "car insurace"
      case 5 => "home insurance"
      case 6 => "life insurance"
      case 7 => "rent"
      case 8 => "leasing"
      case _ => "other"
    }
  }
}

package com.carolina.calvo.domain

import com.carolina.calvo.model.{Client, Geolocation, Transaction, TransactionRecord}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.carolina.calvo.utils.utlis._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.max
import org.apache.spark.util.LongAccumulator

class Domain(sparkSession: SparkSession) {

  import sparkSession.implicits._

  def getDataFromCsv(rddTransactions: DataFrame, sparkSession: SparkSession): (Dataset[Client], Dataset[Transaction]) = {
    val acumulator = sparkSession.sparkContext.longAccumulator("idClient")
    val dfModels = rddTransactions.map(row => {
      acumulator.add(1)
      (
        Client(acumulator.value,
          row.getAs[String]("Name").trim,
          row.getAs[String]("Account_Created").toLong),
        Transaction(acumulator.value,
          parseToDate(row.getAs[String]("Transaction_date"), "M/d/yy h:mm"),
          row.getAs[String]("Price").toDouble,
          row.getAs[String]("Description").trim,
          getCategory(row.getAs[String]("Description").trim),
          row.getAs("Payment_Type")
          ,Geolocation(row.getAs[String]("Latitude").toDouble,row.getAs[String]("Longitude").toDouble, row.getAs[String]("City").trim, "")))
    })

    val dfClients = dfModels.map((row: (Client, Transaction)) =>
      row._1
    )
    val dfTransactions = dfModels.map( row =>
      row._2
    )
    (dfClients, dfTransactions)
  }

  def getData(rddTransactions: RDD[TransactionRecord], acumulator: LongAccumulator): (Dataset[Client], Dataset[Transaction]) = {

    val dfModels = rddTransactions.map(row => {

      acumulator.add(1)
      (
        Client(acumulator.value,
          row.name.trim,
          row.ccc.toLong),
        Transaction(acumulator.value,
          new java.sql.Date(row.transactionDate.getMillis),
          row.price.toDouble,
          row.description.trim,
          getCategory(row.description.trim),
          row.paymentType
          ,Geolocation(row.latitude,row.longitude, row.city.trim, "")))
    })

    val dfClients = dfModels.map((row: (Client, Transaction)) =>
      row._1
    )
    val dfTransactions = dfModels.map( row =>
      row._2
    )
    (dfClients.toDS(), dfTransactions.toDS())
  }

  def getTransactionsPerCity(dfTransactions: Dataset[Transaction]): DataFrame = {

    val dfTransactionsPerCity = dfTransactions.groupBy("geolocation.city").count()
    val dfTransactionsPerCityPretty = dfTransactionsPerCity.withColumnRenamed("count", "transactionsCount")

    // val dfTransactionsPerCitySQL = sparkSession.sql("SELECT geolocation.city, count(*) FROM Transactions GROUP BY geolocation.city")
    dfTransactionsPerCity
  }

  def getClientsAmount500(dfTransactions: Dataset[Transaction], dfClients:Dataset[Client]): DataFrame = {

    val dfClientsAmount500 = dfClients.join(dfTransactions,dfClients("id") === dfTransactions("idClient"), "inner")
      .where("amount > 500")
      .select("name", "ccc")

    /*val dfClientsAmount500SQL = sparkSession.sql("SELECT c.name, c.ccc FROM Transactions t INNER JOIN Clients c ON c.id = t.idClient WHERE t.amount > 500")*/
    dfClientsAmount500
  }

  def getClientsFromLondon(dfTransactions: Dataset[Transaction], dfClients:Dataset[Client]): DataFrame = {
    val dfClientsFromLondon = dfClients.join(dfTransactions, dfClients("id") === dfTransactions("idClient"), joinType = "inner")
      .where("geolocation.city = \"London\"").groupBy(dfClients("name")).count().withColumnRenamed("count", "transactionsCount")

    /*val dfClientsFromLondonSQL =  sparkSession.sql(
      "SELECT c.name, count(*) FROM Transactions t INNER JOIN Clients c ON c.id = t.idClient WHERE t.geolocation.city = \"London\" GROUP BY c.name")*/
    dfClientsFromLondon
  }

  def getTransactionsOcio(dfTransactions: Dataset[Transaction]): DataFrame = {

    val dfTransactionsOcio = dfTransactions.where(dfTransactions("category") === "Ocio")
      .select("idClient", "date", "amount", "description", "category", "geolocation.latitude", "geolocation.longitude", "geolocation.city", "geolocation.country")
    /*val dfTransactionsOcioSQL = sparkSession.sql("SELECT idClient, amount, description, creditCardType FROM Transactions WHERE category = \"Ocio\"")*/
    dfTransactionsOcio
  }

  def getLast30DaysTransactions(dfTransactions: Dataset[Transaction], dfClients: Dataset[Client]): DataFrame = {

    val lastTransaction = dfTransactions.select(max(dfTransactions("date"))).first().getAs[java.sql.Date](0)


    val dfLast30DaysTransactions = dfClients.join(dfTransactions, dfClients("id") === dfTransactions("idClient"), joinType = "inner")
      .where("DATEDIFF(date, \""  + lastTransaction.toString + "\") < 30")
      .groupBy(dfClients("name"))
      .count()
      .withColumnRenamed("count", "transactionsCount")

    /*val dfLast30DaysTransactionsSQL =
      sparkSession.sql("SELECT c.name, count(*) AS transactionCount FROM Transactions t INNER JOIN Clients c ON c.id = t.idClient WHERE DATEDIFF(date, \""  + lastTransaction.toString + "\") < 30 GROUP BY c.name")*/
    dfLast30DaysTransactions
  }

  def getTransactionsWithCountry(dfTransactions: Dataset[Transaction]): DataFrame = {
    val dfTransactionsWithCountry = dfTransactions.map( tr => {
      tr.geolocation.country = getCountry(tr.geolocation.latitude, tr.geolocation.longitude)
      tr
    })
      //.select("idClient", "date", "amount", "description", "category", "geolocation.latitude", "geolocation.longitude", "geolocation.city", "geolocation.country")
    dfTransactionsWithCountry.toDF()
  }

  def getTransactionsPerCountry(dfTransactionsWithCountry: DataFrame): DataFrame = {

    val dfTransactionsPerCountry = dfTransactionsWithCountry.groupBy("geolocation.country").count().withColumnRenamed("count", "transactionsCount")

    // val dfTransactionsPerCountrySQL = sparkSession.sql("SELECT geolocation.country, count(*) FROM Transactions GROUP BY geolocation.country,")
    dfTransactionsPerCountry
  }

  def getTransactionsPerPaymentType(dfTransactions: Dataset[Transaction]): DataFrame = {

    val dfTransactionsPerPaymentType = dfTransactions.groupBy("creditCardType").count().withColumnRenamed("count", "transactionsCount")

    // val dfTransactionsPerPaymentTypeSQL = sparkSession.sql("SELECT creditCardType, count(*) FROM Transactions GROUP BY creditCardType")
    dfTransactionsPerPaymentType
  }
}

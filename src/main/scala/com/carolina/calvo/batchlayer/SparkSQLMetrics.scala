package com.carolina.calvo.batchlayer

import com.carolina.calvo.domain.Domain
import org.apache.spark.sql._


object SparkSQLMetrics {

  def run(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]")
      .appName("Batch Layer")
      .getOrCreate()

    val domain = new Domain(sparkSession)
    val rddTransactions = sparkSession.read.option("header", "true").csv(s"file:///${args(0)}")

    val dfTransactions = domain.getDataFromCsv(rddTransactions, sparkSession)

    val dfTransactionsPerCity = domain.getTransactionsPerCity(dfTransactions)
    dfTransactionsPerCity.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${args(1).toString}/TransaccionesPorCiudad.csv")

    val dfClientsAmount500 = domain.getClientsAmount500(dfTransactions)
    dfClientsAmount500.write.mode(SaveMode.Overwrite).orc(s"${args(1).toString}/ClientesCompras500.orc")


    val dfClientsFromLondon = domain.getClientsFromLondon(dfTransactions)
    dfClientsFromLondon.write.mode(SaveMode.Overwrite).parquet(s"${args(1).toString}/ClientesDeLondres.parquet")


    val dfTransactionsOcio = domain.getTransactionsOcio(dfTransactions)
    dfTransactionsOcio.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${args(1).toString}/TransaccionesOcio.csv")


    val dfLast30DaysTransactions = domain.getLast30DaysTransactions(dfTransactions)
    dfLast30DaysTransactions.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${args(1).toString}/TransaccionesUltimos30Dias.csv")


    val dfTransactionsWithCountry: DataFrame = domain.getTransactionsWithCountry(dfTransactions)
    dfTransactionsWithCountry.coalesce(1).write.mode(SaveMode.Overwrite).parquet(s"${args(1).toString}/TransaccionesConPais.parquet")

    val dfTransactionsPerPaymentType = domain.getTransactionsPerPaymentType(dfTransactions)
    dfTransactionsPerPaymentType.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${args(1).toString}/TransactionesPorTipoTarjeta.csv")

    val dfTransactionsPerCountry = domain.getTransactionsPerCountry(dfTransactionsWithCountry)
      dfTransactionsPerCountry.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${args(1).toString}/TransacionesPorPais")

    sparkSession.close()
  }


}

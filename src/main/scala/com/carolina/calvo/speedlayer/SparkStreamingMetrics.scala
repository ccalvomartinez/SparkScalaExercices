package com.carolina.calvo.speedlayer

import com.carolina.calvo.domain.Domain
import com.carolina.calvo.model.TransactionRecord
import com.carolina.calvo.serdes.TransactionRecordDeserializer
import com.carolina.calvo.speedlayer.generator.KafkaTransactionProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

object SparkStreamingMetrics {
  def run(args: Array[String]): Unit = {




    val sparkSession = SparkSession.builder().master("local[*]")
      .appName("Speed Layer")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    val acumulator = sparkSession.sparkContext.longAccumulator("idClient")

    val kafkaProducer = new KafkaResultsProducer

    val inputTopic = args(1)
    println(inputTopic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[TransactionRecordDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    val inputStream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, TransactionRecord](Array(inputTopic), kafkaParams))

    inputStream.foreachRDD { rdd =>

      val domain = new Domain(sparkSession )

      val lines: RDD[TransactionRecord] = rdd.map(record => record.value)


      val dfTransactions = domain.getData(lines, acumulator)

      val dfTransactionsPerCity = domain.getTransactionsPerCity(dfTransactions)
      if (dfTransactionsPerCity.count()>0){
        dfTransactionsPerCity.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv(s"${args(0).toString}/TransaccionesPorCiudad.csv")
      }
      dfTransactionsPerCity.foreach(row => {
        kafkaProducer.send(args(2), row.toString())
      })

      val dfClientsAmount500 = domain.getClientsAmount500(dfTransactions)
      if (dfClientsAmount500.count() > 0) {
        dfClientsAmount500.write.mode(SaveMode.Append).orc(s"${args(0).toString}/ClientesCompras500.orc")
      }
      dfClientsAmount500.foreach(row => {
        kafkaProducer.send(args(3), row.toString())
      })

      val dfClientsFromLondon = domain.getClientsFromLondon(dfTransactions)
      if(dfClientsFromLondon.count() > 0) {
        dfClientsFromLondon.write.mode(SaveMode.Append).parquet(s"${args(0).toString}/ClientesDeLondres.parquet")
      }
      dfClientsFromLondon.foreach(row => {
        kafkaProducer.send(args(4), row.toString())
      })


      val dfTransactionsOcio = domain.getTransactionsOcio(dfTransactions)
      if (dfTransactionsOcio.count() > 0) {
        dfTransactionsOcio.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv(s"${args(0).toString}/TransaccionesOcio.csv")
      }

      dfTransactionsOcio.foreach(row => {
        kafkaProducer.send(args(5), row.toString())
      })

      val dfLast30DaysTransactions = domain.getLast30DaysTransactions(dfTransactions)
      if(dfLast30DaysTransactions.count() > 0){
        dfLast30DaysTransactions.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv(s"${args(0).toString}/TransaccionesUltimos30Dias.csv")
      }
      dfLast30DaysTransactions.foreach(row => {
        kafkaProducer.send(args(6), row.toString())
      })

      val dfTransactionsWithCountry: DataFrame = domain.getTransactionsWithCountry(dfTransactions)
      if(dfTransactionsWithCountry.count() > 0) {
        dfTransactionsWithCountry.coalesce(1).write.mode(SaveMode.Append).parquet(s"${args(0).toString}/TransaccionesConPais.parquet")
      }
      dfTransactionsWithCountry.foreach(row => {
        kafkaProducer.send(args(7), row.toString())
      })

      val dfTransactionsPerCountry = domain.getTransactionsPerCountry(dfTransactionsWithCountry)
      if(dfTransactionsPerCountry.count() > 0) {
        dfTransactionsPerCountry.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv(s"${args(0).toString}/TransacionesPorPais.csv")
      }
      dfTransactionsPerCountry.foreach(row => {
        kafkaProducer.send(args(8), row.toString())
      })

      val dfTransactionsPerPaymentType = domain.getTransactionsPerPaymentType(dfTransactions)
      if(dfTransactionsPerPaymentType.count() > 0) {
        dfTransactionsPerPaymentType.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv(s"${args(0).toString}/TransactionesPorTipoTarjeta.csv")
      }
      dfTransactionsPerPaymentType.foreach(row => {
        kafkaProducer.send(args(9), row.toString())
      })
     
    }



    Future {
      Thread.sleep(40000)
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
    ssc.start()
    val generator = new KafkaTransactionProducer(inputTopic)
    generator.produceMessages()
    ssc.awaitTermination()


  }
}

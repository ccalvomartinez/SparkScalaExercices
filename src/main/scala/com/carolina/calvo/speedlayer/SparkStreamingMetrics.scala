package com.carolina.calvo.speedlayer

import com.carolina.calvo.domain.Domain
import com.carolina.calvo.model.TransactionRecord
import com.carolina.calvo.serdes.TransactionRecordDeserializer
import com.carolina.calvo.speedlayer.generator.KafkaTransationProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

object SparkStreamingMetrics {
  def run(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaEjercicios")


    val inputTopic = args(0)


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[TransactionRecordDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    val sparkSession = SparkSession.builder().master("local[*]")
      .appName("Speed Layer")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    val acumulator = sparkSession.sparkContext.longAccumulator("idClient")


    val inputStream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, TransactionRecord](Array(inputTopic), kafkaParams))

    inputStream.foreachRDD { rdd =>

      val domain = new Domain(sparkSession )

      val lines: RDD[TransactionRecord] = rdd.map(record => record.value)


      val dfModels = domain.getData(lines, acumulator)



     val dfClients = dfModels._1
      val dfTransactions = dfModels._2

      val dfTransactionsPerCity = domain.getTransactionsPerCity(dfTransactions)
      dfTransactionsPerCity.write.mode(SaveMode.Append).option("header", "true").csv(s"${args(1).toString}/TransaccionesPorCiudad.csv")

      val dfClientsAmount500 = domain.getClientsAmount500(dfTransactions, dfClients)
      println("ENTRADA - Clientes compras mayor 500")
      println("SALIDA - Clientes compras mayor 500", dfClientsAmount500.show(10))

      val dfClientsFromLondon = domain.getClientsFromLondon(dfTransactions, dfClients)
      println("ENTRADA - Clientes Londres")
      println("SALIDA - Clientes Londres", dfClientsFromLondon.show(10))


      val dfTransactionsOcio = domain.getTransactionsOcio(dfTransactions)
      println("ENTRADA - Transacciones ocio")
      println("SALIDA - Transacciones ocio", dfTransactionsOcio.show(10))


      val dfLast30DaysTransactions = domain.getLast30DaysTransactions(dfTransactions, dfClients)
      println("ENTRADA - Transacciones de los últimos 30 días")
      println("SALIDA - Transacciones de los últimos 30 días",dfLast30DaysTransactions.show(10))


      val dfTransactionsWithCountry = domain.getTransactionsWithCountry(dfTransactions)
      println("ENTRADA - Transacciones con pais")
      println("SALIDA - Transacciones con pais", dfTransactionsWithCountry.show(10))
    }


    /*val broker = "localhost:9092"
    val properties = new Properties()
    properties.put("bootstrap.servers", broker)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val outputTopic="mensajesSalida"
> row.

    val producer = new KafkaProducer[String, String](properties)

    processedStream.foreachRDD(rdd =>

      rdd.foreach {
        data: String => {
          val message = new ProducerRecord[String, String](outputTopic, data)
          producer.send(message).get().toString
        }
      })*/

    Future {
      Thread.sleep(40000)
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
    ssc.start()
    val generator = new KafkaTransationProducer(args(0))
    generator.produceMessages()
    ssc.awaitTermination()


  }
}

package com.carolina.calvo.speedlayer.generator

import com.carolina.calvo.model.TransactionRecord
import com.carolina.calvo.serdes.TransactionRecordSerializer
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}


class KafkaTransactionProducer(inputTopic: String) {

  def produceMessages(): Unit = {

    val producer = new KProducer[String, TransactionRecord](classOf[TransactionRecordSerializer].getName)


    for (a <- 1 to 50) {
      val timestamp = DateTime.now().getMillis

      Thread.sleep(1000)
      val message = TransactionGeneratorRandom.generateRandomTransactionRecord()
      Try(producer.produce(inputTopic, "1", message))
      match {
        case Success(m) =>
          val metadata = m.get()
          println(s"Success writing to Kafka topic ${metadata.topic()}, ${metadata.offset()}, ${metadata.partition()}, ${new DateTime(metadata.timestamp())}")

        case Failure(f) => println("Failed writingto Kafka", f.printStackTrace())
      }
    }
  }
}

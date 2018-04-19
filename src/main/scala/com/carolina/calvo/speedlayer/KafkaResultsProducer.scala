package com.carolina.calvo.speedlayer

import com.carolina.calvo.speedlayer.generator.KProducer
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

class KafkaResultsProducer extends Serializable {
  val producer = new KProducer[String, String]("org.apache.kafka.common.serialization.StringSerializer")
  def send(topic:String, message: String): Unit = {
    Try(producer.produce(topic, "1", message))
    match {
      case Success(m) =>
        val metadata = m.get()
        println(s"Success writing to Kafka topic ${metadata.topic()}, ${metadata.offset()}, ${metadata.partition()}, ${new DateTime(metadata.timestamp())}")

      case Failure(f) => println("Failed writingto Kafka", f.printStackTrace())
    }
  }
}

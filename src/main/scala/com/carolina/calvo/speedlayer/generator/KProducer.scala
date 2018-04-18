package com.carolina.calvo.speedlayer.generator

import java.util.Properties
import java.util.concurrent.Future

import com.carolina.calvo.model.TransactionRecord

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}


class KProducer {

  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.carolina.calvo.serdes.TransactionRecordSerializer")

  private lazy val producer = new KafkaProducer[String, TransactionRecord](kafkaProps)

  def produce(topic: String, key: String, value: TransactionRecord, partition: Int = 0): Future[RecordMetadata] = {
    val record = new ProducerRecord(topic, key, value)
    producer.send(record)
  }
}

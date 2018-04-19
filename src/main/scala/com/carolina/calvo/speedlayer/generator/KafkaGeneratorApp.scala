package com.carolina.calvo.speedlayer.generator

object KafkaGeneratorApp extends App {
  val generator = new KafkaTransactionProducer("transaccionesPrueba")
  generator.produceMessages()
}

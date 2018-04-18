package com.carolina.calvo.serdes

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}
import java.util

import com.carolina.calvo.model.TransactionRecord
import org.apache.kafka.common.serialization.Serializer

class TransactionRecordSerializer extends Serializer[TransactionRecord]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, transactionRecord: TransactionRecord): Array[Byte] = {
    var bytes: Array[Byte] = null
    var bos: ByteArrayOutputStream = null
    var oos: ObjectOutputStream = null
    try{
      bos = new ByteArrayOutputStream()
      oos = new ObjectOutputStream(bos)
      oos.writeObject(transactionRecord)
      oos.flush()
      bytes = bos.toByteArray
      bytes
    } catch {
      case e: IOException =>
        e.printStackTrace()
        bytes
    }
  }

  override def close(): Unit = {}
}

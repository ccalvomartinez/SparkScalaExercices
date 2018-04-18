package com.carolina.calvo.serdes

import java.io.{ByteArrayInputStream, IOException, ObjectInputStream}
import java.util

import com.carolina.calvo.model.TransactionRecord
import org.apache.kafka.common.serialization.Deserializer

class TransactionRecordDeserializer extends Deserializer[TransactionRecord]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): TransactionRecord = {
    var obj:Object = null
    var bis: ByteArrayInputStream = null
    var ois: ObjectInputStream = null
    try{
      bis = new ByteArrayInputStream(data)
      ois = new ObjectInputStream(bis)
      obj = ois.readObject()
    } catch {
      case e: IOException => e.printStackTrace()
      case e: ClassNotFoundException => e.printStackTrace()
    }
    obj.asInstanceOf[TransactionRecord]
  }

  override def close(): Unit = {}
}

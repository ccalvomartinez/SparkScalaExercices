package com.carolina.calvo

import com.carolina.calvo.speedlayer.SparkStreamingMetrics

object SpeedLayerApp extends App {
  if (args.length != 10) {
    println("Numero de argumentos incorrecto")
  } else {
    SparkStreamingMetrics.run(args)
  }

}

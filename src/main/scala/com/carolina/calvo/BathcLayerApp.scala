package com.carolina.calvo

import java.nio.file.{Paths, Files}

import com.carolina.calvo.batchlayer.SparkSQLMetrics



object BathcLayerApp extends App {
  if (args.length != 2) {
    println("Número de argumentos incorrecto")
  } else if (!Files.exists(Paths.get(args(0)))) {
      println("No se han encontrado los datos de entrada")
  } else {
    SparkSQLMetrics.run(args)
  }

}

package com.VehicleStream.utils

import java.io.{FileInputStream, IOException}
import java.util.Properties

class PropertiesReader {
  @throws(classOf[IOException])
  def readConfig(filePath: String): Properties = {
    try {
      val properties = new Properties()
      println(s"** Config file read from ${filePath}")
      properties.load(new FileInputStream(filePath))

      return properties;
    }

    catch {
      case ex: Throwable => {(println ("error in reading config", ex));new Properties()}
    }
  }
}
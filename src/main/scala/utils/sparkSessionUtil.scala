package com.VehicleStream.utils

import org.apache.spark.sql.SparkSession

class SparkSessionUtil {

  def getOrCreateSparkSession(sparkConfigPath: String): SparkSession = {
    println(s"using spark config from ${sparkConfigPath}")
    val config = new PropertiesReader().readConfig(sparkConfigPath)
    if (config.getProperty("mode").toLowerCase == "prod") {
      val spark = SparkSession.builder()
        .appName(config.getProperty("app_name"))
        .master(config.getProperty("spark_master")).getOrCreate()
      return spark
    }
    else if (config.getProperty("mode").toLowerCase == "dev") {
      val spark = SparkSession.builder()
        .appName(config.getProperty("app_name"))
        .master("local").getOrCreate()
      return spark
    }
    else {
      val spark = SparkSession.builder()
        .appName(config.getProperty("app_name"))
        .master("local").getOrCreate()
      return spark
    }
  }

}

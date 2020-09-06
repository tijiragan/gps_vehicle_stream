package com.VehicleStream

import org.apache.logging.log4j.scala.{Logging}
import org.apache.log4j.{Logger,PropertyConfigurator}
import org.apache.logging.log4j.Level
import com.VehicleStream.process._
import com.VehicleStream.utils._


object VehicleMain  {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SparkSubmit {config_file : 'path to config'}")
      System.exit(1)
    }
    val runParams =  new ParamParser().parseRunparm(args(0))

    val spark = new SparkSessionUtil().getOrCreateSparkSession(runParams.get("spark_config"))

    val config = new PropertiesReader().readConfig(runParams.get("application_config"))
    val logger = Logger.getLogger(getClass.getName)
    PropertyConfigurator.configure(config.getProperty("log4j_properties"));
    logger.info("Application Booting Up")
    try{
      new VehicleStreamProcess().processStream(spark,config,logger);

    }
    catch {
      case e : Exception => logger.error("Error Processing Stream")
    }
  }

}

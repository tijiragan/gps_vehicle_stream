package com.VehicleStream.process
import org.apache.log4j.{Logger,PropertyConfigurator}
import java.util.Properties

import com.VehicleStream.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

class WriterWrapper(spark:SparkSession,config:Properties,logger:Logger) extends Process {

  def writeFile(filePath:String,append:Boolean,data:String): Boolean = {


    val file = new FileIO(spark)

    return file.fileWrite(filePath:String,append:Boolean,data:String) match {
      case Success(true) => {
        true
      }
      case Failure(ex) => {
        logger.error("Failed to write file", ex); false
      }
    }

  }




}

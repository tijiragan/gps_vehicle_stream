package com.VehicleStream.process
import org.apache.log4j.{Logger,PropertyConfigurator}
import java.util.Properties

import com.VehicleStream.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

class ReaderWrapper(spark:SparkSession,config:Properties,logger:Logger) extends Process {

  def readKafkaTopic(format: String="kafka", server: String = "localhost:9092",offset:String = "latest", topic: String): DataFrame = {


    val kafka = new KafkaIO(spark, config.getProperty("hadoopUri"))

    return kafka.kafkaReadTopic(format: String, server: String ,offset:String , topic: String) match {
      case Success(df) => {
        df
      }
      case Failure(ex) => {
        logger.error("Failed to read kafka topic", ex); spark.emptyDataFrame
      }
    }

  }




}

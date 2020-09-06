package com.VehicleStream.utils

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

class KafkaIO(spark: SparkSession, hadoopUri: String) {
  @throws(classOf[IOException])
  def kafkaReadTopic(format: String="kafka", server: String = "localhost:9092",offset:String = "latest", topic: String): Try[DataFrame] = {

    try {
      if (true) {
        return Success(spark .readStream .format(format) .option("kafka.bootstrap.servers", server) .option("startingOffsets", offset ) .option("subscribe", topic) .load())
      }
      else
        return Success(spark.emptyDataFrame)
    }


    catch {
      case ex: Exception => {
        println(s"Error in reading kafka topic")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)
      }
    }
  }



  def kafkaWriteTopic(format: String="kafka", server: String = "localhost:9092",outputmode:String = "append", topic: String): Unit = {
    println(format, outputmode, topic);
    //Todo
  }

}



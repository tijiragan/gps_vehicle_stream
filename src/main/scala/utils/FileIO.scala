package com.VehicleStream.utils

import java.io.{FileWriter, IOException}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class FileIO(spark:SparkSession) {
  @throws(classOf[IOException])
  def fileReadDataFrameWithSchema(format:String,path:String,schema : StructType): DataFrame = {
    if (new java.io.File(s"$path").exists()) {
      println(s"Reading local Dataframe with Schema ${format}",path)
      return spark.read.format(format).schema(schema).load("file://"+path)
    }
    else
      return spark.emptyDataFrame
  }


  def fileWrite(filePath:String,append:Boolean,data:String) : Try[Boolean] = {
    try{
    val fw = new FileWriter(filePath, append);
              fw.write(data);
              fw.close()
    Success(true)
    }
    catch {
      case ex: Exception => {
        println(s"File write unsuccessful")
        Failure(ex)

      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)

      }
    }
  }
}

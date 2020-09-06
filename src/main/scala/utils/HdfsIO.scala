package com.VehicleStream.utils

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

class HdfsIO(spark: SparkSession, hadoopUri: String) {
  @throws(classOf[IOException])
  def hdfsReadDataFrameWithSchema(format: String, path: String, schema: StructType): Try[DataFrame] = {

    try {
      if (true) {
        return Success(spark.read.format(format).schema(schema).load(path))
      }
      else
        return Success(spark.emptyDataFrame)
    }


    catch {
      case ex: Exception => {
        println(s"Path not found")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)
      }
    }
  }

  def hdfsReadDataFrameWithHeader(format: String, path: String): DataFrame = {
    val conf = new Configuration();
    conf.set("fs.defaultFS", hadoopUri)
    val fs = FileSystem.get(conf)
    if (fs.exists(new Path(fs.getUri + path))) {

      return spark.read.format(format).option("header", "true").load(fs.getUri + path)
    }
    else
      return spark.emptyDataFrame
  }

  def hdfsReadRdd(path: String): Try[RDD[String]] = {

    try {

      if (true) {
        return Success(spark.sparkContext.textFile(path))
        // return spark.sparkContext.textFile(fs.getUri + path)
      }
      else
        return Success(spark.sparkContext.emptyRDD)

    } catch {
      case ex: Exception => {
        println(s"Path not found")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)
      }
    }
  }

  def hdfsDataFrameWrite(format: String, path: String, dataframe: DataFrame, mode: String, partitionColSeq: Seq[String]): Unit = {
    println(format, mode, path);
    dataframe.write.format(format).mode(mode).partitionBy(partitionColSeq: _*).save(path)
  }

}



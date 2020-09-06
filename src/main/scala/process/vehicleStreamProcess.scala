package com.VehicleStream.process

import java.io.{FileWriter, IOException, InputStream, OutputStream}
import java.sql.Timestamp

import org.apache.log4j.Logger
import java.util.Properties

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, split, unix_timestamp}
import org.apache.spark.sql.functions._


case class Record(key: Integer, timestamp: java.sql.Timestamp, longitude: Option[Double], latitude: Option[Double], speed: Option[Integer], eventTime: java.sql.Timestamp, angle: Option[Integer], device_id: String)

object Record {
  def apply(rawStr: String, Key: String, timestamp: java.sql.Timestamp): Record = {
    val values = rawStr.split(",")
    Record(Key.toInt, timestamp, Some(values(0).toDouble), Some(values(1).toDouble), Some(values(2).toInt), new Timestamp(values(3).toLong), Some(values(4).toInt), "device_id")
  }
}

class VehicleStreamProcess {
  @throws(classOf[IOException])
  def processStream(spark: SparkSession, config: Properties, logger: Logger): Any = {
    try {
      import spark.implicits._
      val reader = new ReaderWrapper(spark, config, logger)
      val writer = new WriterWrapper(spark, config, logger)
      val logregex = config.getProperty("logregex").r;

      logger.info("reading kafka topic")

      val streamingDf = reader.readKafkaTopic("kafka", config.getProperty("kafka_server"), "latest", config.getProperty("device1_topic")).select(expr("CAST(key AS STRING)"), expr("CAST(value AS STRING)"), expr("timestamp"))


      val attributesDs: Dataset[Record] = streamingDf
        .selectExpr("CAST(value AS STRING),CAST(key AS STRING), timestamp")
        .map(row => Record(row.getString(0), row.getString(1), row.getTimestamp(2)))

      //todo uncomment to test config properties on cluster
      //      val stoppageQuery = attributesDf.withColumn("co_ord",concat_ws("-",col("latitude"),col("longitude"))).withWatermark("eventTime", config.getProperty("watermarkTimeWindow")).groupBy(window($"eventTime",config.getProperty("stoppageTimeWindow"),config.getProperty("slideDuration"))).agg(approx_count_distinct("co_ord").as("dist_lat")).where("dist_lat = 1")
      logger.info("sinking stoppage alerts")
      val stoppageQuery = attributesDs.withColumn("co_ord", concat_ws("-", col("latitude"), col("longitude"))).withWatermark("eventTime", "20 minutes").groupBy(window($"eventTime", "15 minutes", "10 seconds")).agg(approx_count_distinct("co_ord").as("dist_co_ord")).where("dist_co_ord = 1").writeStream.outputMode("append").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        if (batchDf.count > 0) {
          val alertString = s"Stoppage alert ${batchDF.first().getString(0)}"
          writer.writeFile(config.getProperty("alertFilePath"),true,alertString)
        }
        batchDF.unpersist()
      }.start()

      logger.info("sinking overspeeding alerts")
      val overspeedQuery = attributesDs.withWatermark("eventTime", "3 minutes").groupBy(window($"eventTime", "2 minutes", "10 seconds")).agg(avg("speed").alias("avg_speed")).where("speed > 45").writeStream.outputMode("append").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        if (batchDf.count > 0) {

          val alertString = s"Overspeeding alert ${batchDF.first().getString(0)}"
          writer.writeFile(config.getProperty("alertFilePath"),true,alertString)
        }
        batchDF.unpersist()
      }.start()

      //Two implemention attempts for the sudden angle

      val suddenAngleQuery1 = attributesDs.withWatermark("eventTime", "1 minute").groupBy(window($"eventTime", "10 seconds", "1 second")).agg(avg("angle").alias("avg_angle")).writeStream.trigger(Trigger.ProcessingTime("2 seconds")).outputMode("update").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        val w = org.apache.spark.sql.expressions.Window.partitionBy("device_id").orderBy("eventTime")
        val batchdf2 = batchDF.withColumn("angle_lag", lag("angle", 1).over(w)).where("angle_diff > 20 or angle_diff < -20")
        if (batchdf2.count() > 0) {
          val alertString = s"Sudden angle alert ${batchDF.first().getString(0)}"
          writer.writeFile(config.getProperty("alertFilePath"),true,alertString)
        }
        batchDF.unpersist()
      }.start()


      case class angleState(eventTime: Timestamp, angle: Int, angle_diff: Int)
      def updateStateWithEvent(state: angleState, input: Record): angleState = {
        state.angle_diff = (input.angle.get - state.angle).abs

        state
      }

      def calculateAngleLag(user: String,
                            inputs: Iterator[Record],
                            oldState: GroupState[angleState]): angleState = {
        var state: angleState = oldState.get
        // we simply specify an old date that we can compare against and
        // immediately update based on the values in our data

        for (input <- inputs) {
          state = updateStateWithEvent(state, input)
          oldState.update(state)
        }
        state
      }

      val suddenAngleQuery2 = attributesDs
        .groupByKey(_.device_id)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout)(calculateAngleLag)
        .where("angle_diff > 20")
        .writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.persist()
          if (batchDF.count > 0) {
            val alertString = s"Sudden angle alert ${batchDF.first().getString(0)}"
            writer.writeFile(config.getProperty("alertFilePath"),true,alertString)
          }

          batchDF.unpersist()
        }.start()


    }

    catch {
      case ex => logger.error("Error In Stream Process", ex);
    }
  }

}

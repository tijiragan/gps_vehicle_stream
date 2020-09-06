import com .VehicleStream.process._
import org.scalatest.{BeforeAndAfterEach, FunSpec, FunSuite} ;
import org.apache.log4j.{Logger}
import org.apache.spark.sql.functions.{col, expr, split, unix_timestamp}



class VehicleStreamSpec extends FunSpec with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Log Ingest Test Cases") {



    it("Validates kafka topic") {

      val processStreamTest = new VehicleStreamProcess()

      val df = spark .readStream .format("kafka") .option("kafka.bootstrap.servers", "localhost:9092") .option("startingOffsets", "earliest") .option("subscribe", "test1") .load().select(expr("CAST(key AS STRING)"),expr("CAST(value AS STRING)"),expr("timestamp")).writeStream.queryName("q1").outputMode("append").format("memory").start()

      assert(spark.sql("select * from q1").count() > 0)
    }

  }

}


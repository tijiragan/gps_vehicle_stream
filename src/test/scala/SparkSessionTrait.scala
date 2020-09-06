import com.VehicleStream.utils.PropertiesReader
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }
  val logger:Logger = Logger.getLogger("sparkTest")

  val app_config = new PropertiesReader().readConfig("D:\\jars\\vehicle_stream\\src\\test\\scala\\config\\application_config.properties")
}

name := "vehicle_data_streaming"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.1"
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11"  % "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.poi" % "poi" % "3.15-beta2",
  "org.apache.poi" % "poi-ooxml" % "3.15-beta2"
)


package com.VehicleStream.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.poi.ss.usermodel.{ DataFormatter, WorkbookFactory, Row }
import java.io.File
import collection.JavaConversions._

import java.util.Properties


/**
  * Created by arijit on 4/6/19
  */

object xlsKafkaProducer {

  def main(args: Array[String]): Unit = {
    val topic = args(0)//"test1"
    val brokers = args(1) //"localhost:9092"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val producer = new KafkaProducer[String, String](props);


    val f = new File(args(3))
    //val f = new File("/home/ec2/kafka/locDevice2.xlsx")
    val workbook = WorkbookFactory.create(f)
    val sheet = workbook.getSheetAt(0)

    val formatter = new DataFormatter()
    for (row <- sheet) {
      val stringBuilder = new StringBuilder()
      val key = s"${row.getRowNum}"
      for ( cell <- row) {
        stringBuilder.append(s"${formatter.formatCellValue(cell)}"+ ",")
      }
      stringBuilder.deleteCharAt(stringBuilder.length - 1)
      println(s"Key: ${key}",s"Value:  ${stringBuilder}")
      //todo wrap record around Option
      producer.send(new ProducerRecord[String, String](topic,key,stringBuilder.toString))
      println("Message sent: " + stringBuilder)
      Thread.sleep(1000)//stream rate
    }
  }

}
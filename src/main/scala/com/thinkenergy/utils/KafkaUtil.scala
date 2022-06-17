package com.thinkenergy.utils

import com.thinkenergy.configuration.KafkaConfiguration
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import scala.util.Random

object KafkaUtil extends App {
  val KafkaConf = new KafkaConfiguration

  sendRandomData(KafkaConf.sourceTopic)

  def createProducer(): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", KafkaConf.bootstrapServers)
    properties.put("key.serializer", KafkaConf.keySerializer)
    properties.put("value.serializer", KafkaConf.valueSerializer)
    new KafkaProducer[String, String](properties)
  }

  def sendFileData(file: String, topic: String): Unit = {
    val producer: KafkaProducer[String, String] = createProducer()
    val source = Source.fromFile(file)
    val iterator = source.getLines()
    while (iterator.hasNext) {
      val str = iterator.next()
      producer.send(new ProducerRecord[String, String](topic, str))
    }
    producer.close()
  }

  def sendRandomData(topic: String): Unit = {
    val producer: KafkaProducer[String, String] = createProducer()
    var flag = true
    var i = 0
    while (flag) {
      i+=1
      val nums: Int = Random.nextInt(10)
      val nums2: Int = Random.nextInt(10)
      val ts: Long = (new Date).getTime
      val log = s"""{"id":"$nums","name":"$nums2","age":$nums2,"ts":$ts}""".stripMargin
      println(log)
      producer.send(new ProducerRecord(topic,"tests" , log))
      Thread.sleep(1000)
      if (i > 300) { flag = false }
    }

    producer.close()
  }
}

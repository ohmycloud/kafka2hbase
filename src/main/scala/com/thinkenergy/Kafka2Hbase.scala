package com.thinkenergy

import com.thinkenergy.configuration.KafkaConfiguration

import java.util.Properties
import com.thinkenergy.pipline.KafkaEvents
import com.thinkenergy.sinks.{HbaseSink, MysqlSink}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._


object Kafka2Hbase extends App {
  val kafkaConf = new KafkaConfiguration
  // 1) 配置文件
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", kafkaConf.bootstrapServers)
  properties.setProperty("group.id", kafkaConf.groupId)

  // 2) 执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 3）数据源
  val source = KafkaSource.builder()
    .setProperties(properties)
    .setTopics(kafkaConf.sourceTopic)
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema)
    .build()

  val stream: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks(), "pile")

  // 4) 数据处理
  val transformedStream: DataStream[Map[String, String]] = stream.map(new KafkaEvents)

  // 5) sink
  // transformedStream.print()
  transformedStream.addSink(new MysqlSink)
  transformedStream.addSink(new HbaseSink)

  // Sink
  val sink = KafkaSink.builder[String]()
    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setKafkaProducerConfig(properties)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic(kafkaConf.sinkTopic)
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .build()

  stream.sinkTo(sink)

  // 6) execute
  env.execute("kafka sink")
}

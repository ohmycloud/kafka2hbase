package com.thinkenergy

import com.thinkenergy.configuration.KafkaConfiguration
import com.thinkenergy.ddl.KafkaDdl
import com.thinkenergy.model.Person
import com.thinkenergy.serde.KafkaSchema.PersonSerializationSchema
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.table.api.{DataTypes, Table}
import java.time.Duration
import java.util.Properties


object KafkaTable extends App {
  val kafkaConf = new KafkaConfiguration
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.noRestart())

  val tEnv = StreamTableEnvironment.create(env)
  tEnv.executeSql(KafkaDdl.sourceDdl(kafkaConf.sourceTopic, kafkaConf.bootstrapServers, kafkaConf.groupId))
  var tables: Array[String] = tEnv.listTables()
  tables.foreach(println)
  val table: Table = tEnv.sqlQuery("select * from test")

  val dsFromTable: DataStream[Person] = tEnv.toDataStream(
    table,
    DataTypes.STRUCTURED(
      classOf[Person],
      DataTypes.FIELD("ts", DataTypes.BIGINT()),
      DataTypes.FIELD("id", DataTypes.STRING()),
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("age", DataTypes.INT())
    )
  ).assignTimestampsAndWatermarks(
      // assign timestamps and watermarks which are required for event time
      // Flink 1.11 use new interface of assignTimestampsAndWatermarks that accept a WatermarkStrategy
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Person] {
          override def extractTimestamp(t: Person, l: Long): Long = t.ts
        })
    )

  // The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)
  val properties = new Properties()
  properties.setProperty("transaction.timeout.ms", "300000")
  val sinkPerson: KafkaSink[Person] = KafkaSink.builder[Person]
    .setBootstrapServers(kafkaConf.bootstrapServers)
    .setKafkaProducerConfig(properties)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic(kafkaConf.sinkTopic)
      .setValueSerializationSchema(new PersonSerializationSchema(kafkaConf.sinkTopic))
      .build()
    )
    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build()

  dsFromTable.sinkTo(sinkPerson)

  /*
  val ds: DataStream[Row] = tEnv.sqlQuery("select id, name, age from test").toChangelogStream
  val mappedDs: DataStream[String] = ds.map(x => x.getField("name").toString)

  val simpleSink: KafkaSink[String] = KafkaSink.builder[String]
    .setBootstrapServers(kafkaConf.bootstrapServers)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic(kafkaConf.sinkTopic)
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()

  mappedDs.sinkTo(simpleSink)
  */


  env.execute()
}

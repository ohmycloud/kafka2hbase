package com.thinkenergy.configuration

import com.google.inject.Singleton
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Kafka 配置
  */
@Singleton
class KafkaConfiguration extends Serializable{
  private val config:                   Config = ConfigFactory.load()
  lazy    val kafkaConfig:              Config = config.getConfig("kafka")
  lazy    val sourceTopic:              String = kafkaConfig.getString("common.source.topic.name")
  lazy    val sinkTopic:                String = kafkaConfig.getString("common.sink.topic.name")
  lazy    val bootstrapServers:         String = kafkaConfig.getString("common.bootstrap.servers")
  lazy    val groupId:                  String = kafkaConfig.getString("common.group.id")
  lazy    val keySerializer:            String = kafkaConfig.getString("common.key.serializer")
  lazy    val valueSerializer:          String = kafkaConfig.getString("common.value.serializer")
}
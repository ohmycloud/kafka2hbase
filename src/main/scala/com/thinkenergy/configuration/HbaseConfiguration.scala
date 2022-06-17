package com.thinkenergy.configuration

import com.typesafe.config.{Config, ConfigFactory}
import com.google.inject.Singleton

@Singleton
class HbaseConfiguration extends Serializable {
  private val config:      Config = ConfigFactory.load()
  lazy    val hbaseConfig: Config = config.getConfig("hbase")
  lazy    val hbaseUrl:    String = hbaseConfig.getString("zookeeper.quorum")
  lazy    val hbasePort:   String = hbaseConfig.getString("zookeeper.property.clientPort")
}

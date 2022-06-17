package com.thinkenergy.configuration

import com.google.inject.Singleton
import com.typesafe.config.{Config, ConfigFactory}

/**
 * MySQL 配置信息
 */
@Singleton
class MysqlConfiguration extends Serializable {
  private val config:             Config = ConfigFactory.load()
  lazy    val mysqlConf:          Config = config.getConfig("mysql")
  lazy    val jdbcUrl:            String = mysqlConf.getString("jdbcUrl")
  lazy    val jdbcDriver:         String = mysqlConf.getString("jdbcDriver")
  lazy    val jdbcUser:           String = mysqlConf.getString("jdbcUser")
  lazy    val jdbcPassword:       String = mysqlConf.getString("jdbcPassword")
}

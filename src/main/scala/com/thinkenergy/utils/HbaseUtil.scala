package com.thinkenergy.utils

import com.google.inject.Inject
import com.thinkenergy.configuration.HbaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import javax.inject.Singleton

@Singleton
class HbaseUtil @Inject()  extends Serializable {

  val hbaseConf = new HbaseConfiguration
  val hbaseUrl:   String = hbaseConf.hbaseUrl
  val hbasePort:  String = hbaseConf.hbasePort

  def getConfig: Configuration = {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseUrl)
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbasePort)
      conf.set("hbase.defaults.for.version.skip", "true")
      conf
  }

  /**
    * 获取Hbase连接
    *
    * @return Connection
    */
  def getHbaseConn: Connection = {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseUrl)
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbasePort)
      ConnectionFactory.createConnection(conf)
  }

  def closeConnection(connection:Connection): Unit = {
    if(null!=connection) connection.close()
  }
}
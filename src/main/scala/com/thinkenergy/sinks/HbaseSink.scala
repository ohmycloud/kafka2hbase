package com.thinkenergy.sinks

import java.util

import com.thinkenergy.utils.HbaseUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

class HbaseSink extends RichSinkFunction[Map[String, String]] {
  var conf: org.apache.hadoop.conf.Configuration = _
  var conn: Connection          = _
  var table: Table              = _
  var maxSize: Int              = _
  var delayTime: Long           = _
  var lastInvokeTime: Long      = _
  var puts: util.ArrayList[Put] = _

  override def open(parameters: Configuration): Unit = {
    conn           = (new HbaseUtil).getHbaseConn
    table          = conn.getTable(TableName.valueOf("students"))
    maxSize        = 1000
    delayTime      = 5000L
    lastInvokeTime = System.currentTimeMillis()
    puts           = new util.ArrayList[Put]()
  }

  override def invoke(value: Map[String, String], context: SinkFunction.Context): Unit = {
    val stu_id   = value.getOrElse("id", "")
    val stu_name = value.getOrElse("name", "")
    val stu_age  = value.getOrElse("age", "")

    if ("" != stu_id && "" != stu_name && "" != stu_age) {
      val put = new Put(Bytes.toBytes(stu_id))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("id"), Bytes.toBytes(stu_id))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("name"), Bytes.toBytes(stu_name))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("age"), Bytes.toBytes(stu_age))
      puts.add(put)
    }
  }

  override def close(): Unit = {
    if ( puts.toArray.length > 0) {
      table.put(puts)
    }
    if (table != null) table.close()
    if (conn != null) conn.close()
  }
}

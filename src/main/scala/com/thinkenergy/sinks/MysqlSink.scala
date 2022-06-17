package com.thinkenergy.sinks

import java.sql.{Connection, PreparedStatement}

import com.thinkenergy.utils.JdbcUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MysqlSink extends RichSinkFunction[Map[String, String]] {
  var conn: Connection      = _
  var ps: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = (new JdbcUtil).getConn
    // 学生表: 学号, 姓名, 年龄
    ps   = conn.prepareStatement("insert into students (id, name, age) values (?,?,?)")
  }

  override def invoke(value: Map[String, String], context: SinkFunction.Context): Unit = {
    val stu_id: String   = value.getOrElse("id", "")
    val stu_name: String = value.getOrElse("name", "")
    val stu_age: String  = value.getOrElse("age", "")

    if ("" != stu_id && "" != stu_name && "" != stu_age) {
      ps.setString(1, stu_id)
      ps.setString(2, stu_name)
      ps.setInt(3, stu_age.toInt)

      ps.addBatch()
      ps.executeBatch()
      ps.clearBatch()
    }
  }

  override def close(): Unit = {
    ps.close()
    conn.close()
  }
}

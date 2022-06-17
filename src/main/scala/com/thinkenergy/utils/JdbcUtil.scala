package com.thinkenergy.utils

import java.sql._

import com.google.inject.Inject
import com.thinkenergy.configuration.MysqlConfiguration
import javax.inject.Singleton

/**
 * MySQL 工具
 */
@Singleton
class JdbcUtil @Inject() extends Serializable {

  /**
   * 获取数据库连接
   * @return MysqlConnection 返回数据库连接
   */
  def getConn: Connection = {
    val mysqlConf        = new MysqlConfiguration
    val url: String      = mysqlConf.jdbcUrl
    val driver: String   = mysqlConf.jdbcDriver
    val user: String     = mysqlConf.jdbcUser
    val pwd: String      = mysqlConf.jdbcPassword

    Class.forName(driver)
    try {
      DriverManager.getConnection(url, user, pwd) // 返回数据库连接
    } catch {
      case ex: Exception => {
        println("获取数据库连接错误：Exception=" + ex)
      }
        null;
    }
  }

  def close(ps: PreparedStatement, conn: Connection): Unit = {
    try {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    } catch {
      case e: SQLException =>
        println(e.printStackTrace())
    }
  }
}


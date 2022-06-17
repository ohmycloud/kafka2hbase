package com.thinkenergy.ddl

object KafkaDdl {
  def sourceDdl(topic: String, bootstrapServers: String, groupId: String): String =
    s"""
      |CREATE TABLE test (
      |  ts BIGINT,
      |  id STRING,
      |  name STRING,
      |  age Int
      |) WITH (
      |  'connector' = 'kafka',
      |  'topic' = '${topic}',
      |  'properties.bootstrap.servers' = '${bootstrapServers}',
      |  'properties.group.id' = '${groupId}',
      |  'properties.client.id.prefix'= 'test_pre',
      |  'format' = 'json',
      |  'scan.startup.mode' = 'latest-offset'
      |)
      |""".stripMargin
}

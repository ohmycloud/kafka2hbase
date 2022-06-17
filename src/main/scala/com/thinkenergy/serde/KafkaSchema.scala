package com.thinkenergy.serde

import com.thinkenergy.model.Person
import org.apache.flink.api.common.serialization.SerializationSchema

import java.nio.charset.StandardCharsets

object KafkaSchema {
  class PersonSerializationSchema(topic: String) extends SerializationSchema[Person] {
    override def serialize(p: Person): Array[Byte] = {
      s"""{"id":"${p.id}","name":"${p.name}","age":${p.age},"ts":${p.ts}}""".getBytes(StandardCharsets.UTF_8)
    }
  }
}

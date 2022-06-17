package com.thinkenergy.pipline

import org.apache.flink.api.common.functions.RichMapFunction
import com.alibaba.fastjson.{JSON, JSONObject}
import scala.collection.mutable

class KafkaEvents extends RichMapFunction[String, Map[String, String] ]{
  override def map(in: String): Map[String, String] = {
    try {
      if (in != null && !"".equals(in)){
        // 存放 JSON 中的 key, value 键值对儿
        val chargingPileDataMap = new mutable.HashMap[String,String]()
        val chargingPileData: JSONObject = JSON.parseObject(in)
        chargingPileData.keySet().toArray.foreach( f => {
          val field: String = f.toString
          val value: String = chargingPileData.getString(field)
          if ("" != value && "" != field) {
            chargingPileDataMap.put(field, chargingPileData.getString(field))
          }
        })
        if (chargingPileDataMap.nonEmpty) {
          chargingPileDataMap.toMap
        } else {
          Map("" -> "")
        }
      }else{
        Map("" -> "")
      }
    } catch {
      case e: Exception =>
        println(e, in)
        Map("" -> "")
    }
  }
}

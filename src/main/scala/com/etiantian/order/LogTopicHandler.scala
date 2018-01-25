package com.etiantian.order

import org.apache.spark.rdd.RDD
import org.json.JSONObject

class LogTopicHandler(topicName:String) extends MessageOrder with Serializable {

  override def handlerMessage(message:RDD[(String, String)]) = {
    message.filter(x => x._1.equals(topicName))
//      .filter(x => {
//        var rtn = false
//        try {
////          val jsonObject = new JSONObject(x._2)
//
//          rtn = true
//        }
//        rtn
//    })
      .collect().foreach(x => println(topicName+" : " +x))
  }

  override def getTopic() = {
    topicName
  }
}

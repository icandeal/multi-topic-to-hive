package com.etiantian.order

import org.apache.spark.rdd.RDD

abstract class MessageOrder(topicName: String) {

  def handlerMessage(rdd:RDD[(String, String)])

  def getTopic():String = topicName
}

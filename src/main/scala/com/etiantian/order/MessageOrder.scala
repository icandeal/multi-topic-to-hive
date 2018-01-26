package com.etiantian.order

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

trait MessageOrder {

  def handlerMessage(rdd:RDD[(String, String)])

  def getTopic():String
}

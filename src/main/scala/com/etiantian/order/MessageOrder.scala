package com.etiantian.order

import org.apache.spark.rdd.RDD

trait MessageOrder {

  def handlerMessage(rdd:RDD[(String, String)])

  def getTopic():String
}

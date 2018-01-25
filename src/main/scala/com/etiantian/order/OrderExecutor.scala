package com.etiantian.order

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

object OrderExecutor {
  private var orderList = List[MessageOrder]()
  val logger = Logger.getLogger(OrderExecutor.getClass)

  def addOrder(order: MessageOrder) = {
    orderList = orderList :+ order
  }

  def executeOrder(rdd: RDD[(String, String)]): Set[String] = {
    var topicSet = Set[String]()
    orderList.foreach(x => {
      try {
        val topicName = x.getTopic()
        x.handlerMessage(rdd)
        topicSet += topicName
      } catch {
        case e: Exception => logger.error("Execute order error !!", e)
      }
    })
    topicSet
  }
}

package com.etiantian.order

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object OrderExecutor {
  private var orderList = List[MessageOrder]()
  val logger = Logger.getLogger(OrderExecutor.getClass)

  def addOrder(order: MessageOrder) = {
    orderList = orderList :+ order
  }

  def validateOrder(topicSet: Set[String]) = {
    logger.warn("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  validating orders  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    var listBuffer = new ListBuffer[MessageOrder]
    orderList.foreach(obj => {
      if(!topicSet.contains(obj.getTopic())) {
        logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  remove  order  !!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.warn("Order name is : " + obj.getClass)
        logger.warn("Topic name is : " + obj.getTopic())
        logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
      }
      else listBuffer += obj
    })
    orderList = listBuffer.toList
  }

  def executeOrder(rdd: RDD[(String, String)]): Set[String] = {
    var topicSet = Set[String]()
    orderList.foreach(x => {
      try {
        val topicName = x.getTopic()
        x.handlerMessage(rdd)
        topicSet += topicName
      } catch {
        case e: Exception => {
          logger.error("Execute order error !!", e)
          var listBuffer = new ListBuffer[MessageOrder]
          orderList.foreach(obj => {
            if(x == obj) {
              logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  remove order  !!!!!!!!!!!!!!!!!!!!!!!!!!")
              logger.warn("Order name is : " + obj.getClass)
              logger.warn("Topic name is : " + obj.getTopic())
              logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
            }
            else listBuffer += obj
          })
          orderList = listBuffer.toList
        }
      }
    })
    topicSet
  }
}

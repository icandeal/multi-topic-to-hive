package com.etiantian.order
import org.apache.spark.rdd.RDD

class TestHandler(val topicName: String) extends MessageOrder with Serializable {
  override def getTopic(): String = topicName

  override def handlerMessage(rdd: RDD[(String, String)]): Unit = {
    rdd.collect().foreach(println)
  }
}

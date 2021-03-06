package com.etiantian

import java.io.FileInputStream
import java.util.Properties

import com.etiantian.order._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

/**
 * Hello world!
 *
 */
object MultiTopicHandler {

  val logger = Logger.getLogger(MultiTopicHandler.getClass)

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    if (args == null || args.length < 1) {
      throw new Exception("No Config File!!!")
    }
    properties.load(new FileInputStream(args(0)))
    val groupId = properties.getProperty("groupId")
    val brokers = properties.getProperty("brokers")
    val topics = properties.getProperty("topics")
    val cycle = properties.getProperty("cycle").toInt
    val offsetReset = properties.getProperty("offsetReset")

//    val groupId = "KafkaCheckPoint111111"
//    val brokers = "t45.test.etiantian.com:9092"
//    val topics = "ycf1,ycf2,ycf3"
//    val cycle = 10
//    val offsetReset = "largest"
//    val offsetReset = "smallest"
    val sparkConf = new SparkConf()
//      .setMaster("local[4]")
      .setAppName("ycf:MultiTopicHandler")
    val ssc = new StreamingContext(sparkConf, Seconds(cycle))

    val kafkaParam = Map(
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId
    )
    //////////////////////////////////////////// 只用修改这里 ///////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    OrderExecutor.addOrder(new TestHandler("ycf1"))
    OrderExecutor.addOrder(new TestHandler("ycf2"))
    OrderExecutor.addOrder(new LogTopicHandler("logTopic"))
    OrderExecutor.addOrder(new ActionLogsHandler("aixueOnline"))
    OrderExecutor.addOrder(new RecommendationHandler("interface_call_logs"))
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    var partionAndOffset = Map[TopicAndPartition,Long]()
    val kafkaCluster = new KafkaCluster(kafkaParam)

    val topicArray = topics.split(",")
    topicArray.foreach(topic => {
      val topicAndPartitionSet = kafkaCluster.getPartitions(Set(topic)).right.get
      //isLeft没有保存offset
      val consumerOffsets = kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id").get, topicAndPartitionSet)
      val earliestOffset = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet).right.get
      val latestOffset = kafkaCluster.getLatestLeaderOffsets(topicAndPartitionSet).right.get
      if (consumerOffsets.isLeft) {
        if(offsetReset!= null && offsetReset.equals("smallest")) {
          earliestOffset.foreach(x => {
            println("###############################  new offset init  ###############################")
            println("topic and partition = " + x._1)
            println("earliest offset = " + x._2.offset)
            println("##################################################################################\n")
            partionAndOffset += (x._1 -> x._2.offset)
          })
        }
        else {
          latestOffset.foreach(x => {
            println("###############################  new offset init  ###############################")
            println("topic and partition = " + x._1)
            println("latest offset = " + x._2.offset)
            println("##################################################################################\n")
            partionAndOffset += (x._1 -> x._2.offset)
          })
        }
      }
      else {
        val consumerPartionAndOffset = consumerOffsets.right.get
        consumerPartionAndOffset.foreach(x =>  {
          val topicAndPartition = x._1
          val offset = x._2
          println("################################  load partition  ################################")
          println("add topic offset : " + topicAndPartition +" => "+ offset)
          println("##################################################################################\n")
          partionAndOffset += (topicAndPartition -> math.max(earliestOffset.get(topicAndPartition).get.offset, offset))
        })
      }
    })

    println("============================ topic and partition ==============================")
    println(partionAndOffset)
    println("===============================================================================\n")

    OrderExecutor.validateOrder(topicArray.toSet)

    val messageHandler = (mmd: MessageAndMetadata[String,  String]) => (mmd.topic, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, partionAndOffset, messageHandler)
    var offsetRanges = Array.empty[OffsetRange]
    kafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).foreachRDD(rdd => {

      val topicSet = OrderExecutor.executeOrder(rdd)
      logger.warn("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
      logger.warn("write topic offset :"+ topicSet.mkString(" && "))
      logger.warn("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
      offsetRanges.foreach(x => {
        if(topicSet.contains(x.topic)) {
          val tap = x.topicAndPartition()
          val map = Map[TopicAndPartition, Long](tap -> x.untilOffset)
          kafkaCluster.setConsumerOffsets(kafkaParam.get("group.id").get, map)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

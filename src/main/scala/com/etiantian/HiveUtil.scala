package com.etiantian

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by yuchunfan on 2017/8/23.
  */
object HiveUtil {

  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    synchronized {
      if (instance == null)
        instance = new HiveContext(sparkContext)
      instance.setConf("hive.exec.dynamic.partition", "true")
      instance.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      instance
    }
  }

  def appendToHiveOrc(rdd:RDD[String], tableName:String, partionBy:String) = {
    if(!rdd.isEmpty()) {
      HiveUtil.getInstance(rdd.sparkContext)
        .read.json(rdd).write.mode(SaveMode.Append).format("orc").partitionBy(partionBy).saveAsTable(tableName)
    }
  }
}

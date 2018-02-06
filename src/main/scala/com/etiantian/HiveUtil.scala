package com.etiantian

import com.etiantian.HiveUtil.instance
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

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
//      val sqlContext = new HiveContext(rdd.sparkContext)
//      sqlContext.setConf("hive.exec.dynamic.partition", "true")
//      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//      val df = sqlContext
        .read.json(rdd)
        .write.mode(SaveMode.Append).format("orc")
        .partitionBy(partionBy)
        .saveAsTable(tableName)
    }
  }

  def appendToHiveOrc(rdd:RDD[Row], structType: StructType, tableName:String, partionBy:String) = {
    if(!rdd.isEmpty()) {
      HiveUtil.getInstance(rdd.sparkContext)
          .createDataFrame(rdd, structType)
        .write.mode(SaveMode.Append).format("orc")
        .partitionBy(partionBy)
        .saveAsTable(tableName)
    }
  }
}

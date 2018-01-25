package com.etiantian.order

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

trait MessageOrder {

//  {
//    "3": "sxlogsdb_zhishidaoxue_action_log",
//    "4": "sxlogsdb_micro_course_point_log",
//    "5": "sxlogsdb_paper_sub_marking_log",
//    "6": "sxlogsdb_paper_answer_option_time_log",
//    "7": "sxlogsdb_paper_answer_stay_time_log",
//    "8": "sxlogsdb_yuxiwang_edition_log",
//    "9": "sxlogsdb_stu_action_data_log",
//    "10": "resourcelogs_resource_use_logs",
//    "20": "sxlogsdb_search_save_log"
//  }

  def handlerMessage(rdd:RDD[(String, String)])

  def getTopic():String
}

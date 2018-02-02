package com.etiantian.order

import java.text.SimpleDateFormat

import com.etiantian.HiveUtil
import org.apache.spark.rdd.RDD
import org.json.JSONObject

class LogTopicHandler(topicName: String) extends MessageOrder(topicName) with Serializable {

  override def handlerMessage(message: RDD[(String, String)]) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val rdd = message.filter(x => x._1.equals(topicName)).filter(x => {
      var rtn = false
      try {
        val json = new JSONObject(x._2)

        rtn = json.has("messageTypeId")
      }
      rtn
    }).cache()

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

    //  messageTypeId == 3
    val zsdxRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("3")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val actionTime = if (json.has("actiontime")) json.getString("actiontime") else ""
        format.parse(actionTime)
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)
        val cDate = cTime.substring(0,10)
//        val messageTypeId = if (json.has("messagetypeid")) json.getInt("messagetypeid") else null
        val messageTypeId = json.getInt("messagetypeid")

        val nodeId = if (json.has("nodeid")) json.getLong("nodeid") else null
        val noderelation = if (json.has("noderelation")) json.getInt("noderelation") else null
        val referenceid = if (json.has("referenceid")) json.getInt("referenceid") else null
        val resourceid = if (json.has("resourceid")) json.getLong("resourceid") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
//        val userid = if (json.has("userid")) json.getLong("userid") else null
        val userid = json.getLong("userid")

        row = new JSONObject()
        row.put("actiontime", actionTime)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)
        row.put("nodeid", nodeId)
        row.put("noderelation", noderelation)
        row.put("referenceid", referenceid)
        row.put("resourceid", resourceid)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("userid", userid)
        row.put("c_date", cDate)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(zsdxRdd, "sxlogsdb_zhishidaoxue_action_log", "c_date")

    //  messageTypeId == 4
    val microCoursePointRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("4")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val actionTime = if (json.has("actiontime")) json.getString("actiontime") else ""
        format.parse(actionTime)
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)
        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val point = if (json.has("point")) json.getLong("point") else null
        val resourceid = if (json.has("resourceid")) json.getLong("resourceid") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null

        row = new JSONObject()
        row.put("actiontime", actionTime)
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)

        row.put("point", point)
        row.put("resourceid", resourceid)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(microCoursePointRdd, "sxlogsdb_micro_course_point_log", "c_date")


    //  messageTypeId == 5 "5": "sxlogsdb_paper_sub_marking_log",
    val paperSubMarkingRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("5")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val actionTime = if (json.has("actiontime")) json.getString("actiontime") else ""
        format.parse(actionTime)
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)
        val cDate = cTime.substring(0,10)
        val messageTypeId = if (json.has("messagetypeid")) json.getInt("messagetypeid") else null

        val paperid = if (json.has("paperid")) json.getLong("paperid") else null
        val questionid = if (json.has("questionid")) json.getLong("questionid") else null
        val score = if (json.has("score")) json.getDouble("score") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null

        row = new JSONObject()
        row.put("actiontime", actionTime)
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)

        row.put("paperid", paperid)
        row.put("questionid", questionid)
        row.put("score", score)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(paperSubMarkingRdd, "sxlogsdb_paper_sub_marking_log","c_date")

    //  messageTypeId == 6 "6": "sxlogsdb_paper_answer_option_time_log",
    val paperAnsewerOptionLogsRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("6")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val answerid = if (json.has("answerid")) json.getLong("answerid") else null
        val chosetime = if (json.has("chosetime")) json.getString("chosetime") else null
        format.parse(chosetime)
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)
        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val paperid = if (json.has("paperid")) json.getLong("paperid") else null
        val questionid = if (json.has("questionid")) json.getLong("questionid") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null

        row = new JSONObject()
        row.put("answerid", answerid)
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("chosetime",chosetime)
        row.put("messagetypeid",messageTypeId)

        row.put("paperid", paperid)
        row.put("questionid", questionid)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(paperAnsewerOptionLogsRdd, "sxlogsdb_paper_answer_option_time_log","c_date")


    //  messageTypeId == 7   "7": "sxlogsdb_paper_answer_stay_time_log",
    val paperAnswerStayTimeRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("7")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)

        val leavetime = if(json.has("leavetime")) json.getString("leavetime") else ""
        format.parse(leavetime)

        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val paperid = if (json.has("paperid")) json.getLong("paperid") else null
        val questionid = if (json.has("questionid")) json.getLong("questionid") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val staytime = if (json.has("staytime")) json.getInt("staytime") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null

        row = new JSONObject()
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("leavetime",leavetime)
        row.put("messagetypeid",messageTypeId)

        row.put("paperid", paperid)
        row.put("questionid", questionid)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("staytime", staytime)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(paperAnswerStayTimeRdd, "sxlogsdb_paper_answer_stay_time_log","c_date")


    //  messageTypeId == 8   "8": "sxlogsdb_yuxiwang_edition_log",
    val yuxiwangEditionLogRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("8")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)

        val savetime = if(json.has("savetime")) json.getString("savetime") else ""
        format.parse(savetime)

        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val sublist = if (json.has("sublist")) json.getString("sublist") else ""
        val subject_id = if (json.has("subject_id")) json.getInt("subject_id") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val subject_type = if (json.has("subject_type")) json.getInt("subject_type") else null
        val version_id = if (json.has("version_id")) json.getInt("version_id") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null


        row = new JSONObject()
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)

        row.put("savetime",savetime)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("subject_id", subject_id)
        row.put("subject_type", subject_type)
        row.put("sublist", sublist)
        row.put("userid", userid)
        row.put("version_id", version_id)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(yuxiwangEditionLogRdd, "sxlogsdb_yuxiwang_edition_log","c_date")


    //  messageTypeId == 9   "9": "sxlogsdb_stu_action_data_log",
    val stuActionDataLogRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("9")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)

        val savetime = if(json.has("savetime")) json.getString("savetime") else ""
        format.parse(savetime)

        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val jid = if (json.has("jid")) json.getLong("jid") else null
        val object_id = if (json.has("object_id")) json.getLong("object_id") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val sub_type = if (json.has("sub_type")) json.getInt("sub_type") else null
        val task_id = if (json.has("task_id")) json.getLong("task_id") else null
        val task_type = if (json.has("task_type")) json.getLong("task_type") else null
        val video_position = if (json.has("video_position")) json.getLong("video_position") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null


        row = new JSONObject()
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("jid", jid)
        row.put("messagetypeid",messageTypeId)

        row.put("object_id", object_id)
        row.put("savetime",savetime)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("sub_type", sub_type)
        row.put("task_id", task_id)
        row.put("task_type", task_type)
        row.put("userid", userid)
        row.put("video_position", video_position)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(stuActionDataLogRdd, "sxlogsdb_stu_action_data_log","c_date")


    //  messageTypeId == 10   "10": "resourcelogs_resource_use_logs",
    val resourceUseLogRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("10")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)

        val action_time = if(json.has("action_time")) json.getString("action_time") else ""
        format.parse(action_time)

        val savetime = if(json.has("savetime")) json.getString("savetime") else ""
        format.parse(savetime)

        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val action_id = if (json.has("action_id")) json.getInt("action_id") else null
        val object_id = if (json.has("object_id")) json.getLong("object_id") else null
        val object_time = if (json.has("object_time")) json.getInt("object_time") else null
        val object_type = if (json.has("object_type")) json.getInt("object_type") else null
        val servid = if (json.has("servid")) json.getInt("servid") else null
        val srcid = if (json.has("srcid")) json.getInt("srcid") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null


        row = new JSONObject()
        row.put("action_id", action_id)
        row.put("action_time", action_time)
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)

        row.put("object_id", object_id)
        row.put("object_time", object_time)
        row.put("object_type", object_type)
        row.put("savetime",savetime)
        row.put("servid", servid)
        row.put("srcid", srcid)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(resourceUseLogRdd, "resourcelogs_resource_use_logs","c_date")


    //  messageTypeId == 20  "20": "sxlogsdb_search_save_log"
    val searchSaveLogsRdd = rdd.filter(x => {
      val json = new JSONObject(x._2)
      json.getString("messageTypeId").equals("20")
    }).map(x => {
      val json = new JSONObject(x._2.toLowerCase())
      var row:JSONObject = null
      try {
        val cTime = if (json.has("c_time")) json.getString("c_time") else ""
        format.parse(cTime)

        val cDate = cTime.substring(0,10)
        val messageTypeId = json.getInt("messagetypeid")

        val appid = if (json.has("appid")) json.getInt("appid") else null
        val pointid = if (json.has("pointid")) json.getLong("pointid") else null
        val resourceid = if (json.has("resourceid")) json.getLong("resourceid") else null
        val searchid = if (json.has("searchid")) json.getLong("searchid") else null
        val thetype = if (json.has("type")) json.getInt("type") else null
        val userid = if (json.has("userid")) json.getLong("userid") else null


        row = new JSONObject()
        row.put("appid", appid)
        row.put("c_date", cDate)
        row.put("c_time", cTime)
        row.put("messagetypeid",messageTypeId)

        row.put("pointid", pointid)
        row.put("resourceid", resourceid)
        row.put("searchid", searchid)
        row.put("type", thetype)
        row.put("userid", userid)

      } catch {
        case e:Exception =>{}
      }
      if(row != null) row.toString() else null
    }).filter(x => x!=null)

    HiveUtil.appendToHiveOrc(searchSaveLogsRdd, "sxlogsdb_search_save_log","c_date")
  }
}

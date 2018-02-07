package com.etiantian.order
import java.text.SimpleDateFormat

import com.etiantian.HiveUtil
import org.apache.spark.rdd.RDD
import org.json.JSONObject

class ActionLogsHandler(val topicName: String) extends MessageOrder with Serializable  {

  override def getTopic(): String = topicName

  override def handlerMessage(rdd: RDD[(String, String)]): Unit = {
    val jsonRdd = rdd.filter(x => x._1.equals(topicName)).map(x => {
      var rtn: String = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      try {
        val json = new JSONObject(x._2)
        val jsonObject = new JSONObject()
        val cTime = json.get("c_time").toString
        format.parse(cTime)
        jsonObject.put("01", cTime)
        jsonObject.put("02", json.get("cost_time").toString.toInt)

        if (!json.has("ettFaceCostTimeJson"))
          jsonObject.put("03", "")
        else
          jsonObject.put("03", json.get("ettFaceCostTimeJson").toString)

        if (!json.has("ip"))
          jsonObject.put("04", "")
        else
          jsonObject.put("04", json.get("ip").toString)
        jsonObject.put("05", json.get("jid").toString.toInt)
        if (!json.has("messageFrom"))
          jsonObject.put("06", "")
        else
          jsonObject.put("06", json.get("messageFrom").toString)

        if (!json.has("messageTypeId"))
          jsonObject.put("07", "")
        else
          jsonObject.put("07", json.get("messageTypeId").toString)

        jsonObject.put("08", json.get("model_name").toString)

        if (!json.has("param_json"))
          jsonObject.put("09", "")
        else
          jsonObject.put("09", json.get("param_json").toString)

        jsonObject.put("10", json.get("project_name").toString)

        if (!json.has("school_id"))
          jsonObject.put("11", "")
        else
          jsonObject.put("11", json.get("school_id").toString.toInt)

        if (!json.has("session_id"))
          jsonObject.put("12", "")
        else
          jsonObject.put("12", json.get("session_id").toString)
        if (!json.has("token"))
          jsonObject.put("13", "")
        else
          jsonObject.put("13", json.get("token").toString)

        jsonObject.put("14", json.get("url").toString)

        if (!json.has("user_id"))
          jsonObject.put("15", "")
        else
          jsonObject.put("15", json.get("user_id").toString)
        jsonObject.put("c_date", cTime.substring(0, 10))

        rtn = jsonObject.toString
      } catch {
        case e: Exception => {
          println(e)
          println("====================== (" + x._1 + ":" + x._2 + ")======================")
        }
      }
      rtn
    }).filter(_ != null)

    HiveUtil.appendToHiveOrc(jsonRdd, "sxlogsdb_action_logs", "c_date")
  }

}

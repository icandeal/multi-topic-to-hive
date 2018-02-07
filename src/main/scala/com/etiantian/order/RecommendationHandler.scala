package com.etiantian.order
import java.text.SimpleDateFormat

import com.etiantian.HiveUtil
import org.apache.spark.rdd.RDD
import org.json.JSONObject

class RecommendationHandler(val topicName: String) extends MessageOrder with Serializable {

  override def getTopic(): String = topicName

  override def handlerMessage(rdd: RDD[(String, String)]): Unit = {
    val reRdd = rdd.filter(_._1.equals(topicName)).map(x => {
      var rtn: String = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      try {
        val json = new JSONObject(x._2)
        val jsonObject = new JSONObject()
        val cTime = json.get("c_time").toString
        format.parse(cTime)
        jsonObject.put("01", cTime)
        jsonObject.put("02", if (json.has("class")) json.getString("class") else "")
        jsonObject.put("03", if (json.has("method")) json.getString("method") else "")
        jsonObject.put("04", if (json.has("params")) json.getString("params") else "")
        jsonObject.put("05", if (json.has("returnVal")) json.getString("returnVal") else "")
        jsonObject.put("06", if (json.has("url")) json.getString("url") else "")
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

    HiveUtil.appendToHiveOrc(reRdd, "recommendation_action_logs", "c_date")
//    throw new Exception("dont't save the offset!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  }
}

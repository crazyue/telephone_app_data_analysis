package com.atguigu.utils

import com.atguigu.model.StartupReportLogs
import org.codehaus.jackson.map.ObjectMapper

object JsonUtils{
  def json2StartupLog(json:String) ={
    val mapper = new ObjectMapper()
    val obj = mapper.readValue(json, classOf[StartupReportLogs])
    obj
  }
}

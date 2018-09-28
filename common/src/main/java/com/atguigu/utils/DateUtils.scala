package com.atguigu.utils

import java.text.SimpleDateFormat
import java.util.Date

object  DateUtils{
  def dateToString(date:Date): String ={
    val dateString = new SimpleDateFormat("yyyy-MM-dd")
    val dateStr = dateString.format(date)
    dateStr
  }
}
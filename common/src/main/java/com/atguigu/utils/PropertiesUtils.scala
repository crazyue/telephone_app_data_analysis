package com.atguigu.utils

import java.util.Properties

object PropertiesUtils{
  def getProperties():Properties = {
    val properties = new Properties()
    val in = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in)
    properties
  }

  def loadProperties(key:String):String = {
    val properties = new Properties()
    val in = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in)
    properties.getProperty(key)
  }
}
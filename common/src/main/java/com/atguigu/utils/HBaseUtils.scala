package com.atguigu.utils

import java.util.Properties

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory

object HBaseUtils{
  def getHBaseTabel(prop: Properties) = {
    // 创建HBase配置
    val config = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort", PropertiesUtils.loadProperties("hbase.zookeeper.property.clientPort"))
    config.set("hbase.zookeeper.quorum", PropertiesUtils.loadProperties("hbase.zookeeper.quorum"))
    // 创建HBase连接
    val connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    val table = connection.getTable(TableName.valueOf("online_city_click_count"))
    table
  }
}
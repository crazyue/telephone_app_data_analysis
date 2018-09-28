package com.atguigu.online

import java.util.Date
import com.atguigu.utils._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object OnlineScala {

  def main(args: Array[String]): Unit = {
    // StreamingContext.getActiveOrCreate(checkpointPath, func),利用这种方式创建
    //的streamingContext,如果挂掉了，会去checkPoint的路径下利用恢复文件来恢复
    //streamingContext,如果没有找到恢复文件就利用func函数重新生成一个streamingContext
    //还有一中val ssc = new StreamingContext(conf, Seconds(5))
    val streamingContext = StreamingContext.getActiveOrCreate(PropertiesUtils.loadProperties("streaming.checkpoint.path"),
      createContextFunc())

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createContextFunc(): () => _root_.org.apache.spark.streaming.StreamingContext = {
    () => {
      // 创建sparkConf
      val sparkConf = new SparkConf().setAppName("online").setMaster("local[*]")
      // 配置sparkConf优雅的停止
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 配置Spark Streaming每秒钟从kafka分区消费的最大速率
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 指定Spark Streaming的序列化方式为Kryo方式
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 指定Kryo序列化方式的注册器
      sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")

      // 创建streamingContext
      val interval = PropertiesUtils.loadProperties("streaming.interval")
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval.toLong))
      // 启动checkpoint
      val checkPointPath = PropertiesUtils.loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      // 获取kafka配置参数
      val kafka_brokers = PropertiesUtils.loadProperties("kafka.broker.list")
      val kafka_topic = PropertiesUtils.loadProperties("kafka.topic")
      var kafka_topic_set : Set[String] = Set(kafka_topic)
      val kafka_group = PropertiesUtils.loadProperties("kafka.groupId")

      // 创建kafka配置参数Map
      val kafkaParam = Map(
        "bootstrap.servers" -> kafka_brokers,
        "group.id" -> kafka_group
      )

      // 创建kafkaCluster
      val kafkaCluster = new KafkaCluster(kafkaParam)
      // 获取Zookeeper上指定主题分区的offset
      // topicPartitionOffset: Map[(TopicAndPartition, offset)] TopicAndPartition的结构为（topic: String, partition: Int）
      val topicPartitionOffset = ZookeeperUtils.getOffsetFromZookeeper(kafkaCluster, kafka_group, kafka_topic_set)

      // 创建DirectDStream
      // KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParam, kafka_topic_set)
      //[]中的类型分别为：kafka message的key类型，value类型，key解码，value解码，mess的类型
      // (mess: MessageAndMetadata[String, String]) => mess.message()的作用是只提取kafka消息中的value，舍弃key
      //这里自传入的offset处继续消费
      val onlineLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](streamingContext, kafkaParam, topicPartitionOffset, (mess: MessageAndMetadata[String, String]) => mess.message())

      // checkpoint原始数据 onlineLogDStream类型DStream[RDD[String]]
      onlineLogDStream.checkpoint(Duration(10000))

      // 过滤垃圾数据
      val onlineFilteredDStream = onlineLogDStream.filter{
        case message =>
          var success = true

          if(!message.contains("appVersion") && !message.contains("currentPage") &&
            !message.contains("errorMajor")){
            success = false
          }

          if(message.contains("appVersion")){
            val startupReportLog = JsonUtils.json2StartupLog(message)
            if(startupReportLog.getUserId == null || startupReportLog.getAppId == null)
              success = false
          }
          success
      }

      // 完成需求统计并写入HBase
      onlineFilteredDStream.foreachRDD{
        // {"activeTimeInMs":749182,"appId":"app00001","appPlatform":"ios","appVersion":"1.0.1","city":"Hunan","startTimeInMs":1534301120672,"userId":"user116"}
        rdd => rdd.foreachPartition{
          items =>
            val table = HBaseUtils.getHBaseTabel(PropertiesUtils.getProperties())
            while(items.hasNext){
              val item = items.next()
              val startupReportLog = JsonUtils.json2StartupLog(item)
              val date = new Date(startupReportLog.getStartTimeInMs)
              // yyyy-MM-dd
              val dateTime = DateUtils.dateToString(date)
              val rowKey = dateTime + "_" + startupReportLog.getCity
              table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("StatisticData"), Bytes.toBytes("userNum"), 1L)
              println(rowKey)
            }
        }
      }

      // 完成需求统计后更新Zookeeper数据
      ZookeeperUtils.offsetToZookeeper(onlineLogDStream, kafkaCluster, kafka_group)

      streamingContext
    }
  }

}

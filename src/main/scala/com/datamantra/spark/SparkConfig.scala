package com.datamantra.spark

import com.datamantra.config.Config
import com.datamantra.kafka.KafkaConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

/**
  * Created by kafka on 9/5/18.
  */
object SparkConfig {
   val logger = Logger.getLogger(getClass.getName)

   val sparkConf = new SparkConf

   var transactionDatasouce:String = _
   var customerDatasource:String = _
   var modelPath:String = _
   var preprocessingModelPath:String = _
   var shutdownMarker:String = _

   def loadCommonConfig() = {

     sparkConf.setAppName(Config.applicationConf.getString("config.common.spark.name"))
       .set("spark.streaming.stopGracefullyOnShutdown", Config.applicationConf.getString("config.common.spark.gracefulShutdown"))
   }

   def loadLocalConfig() = {

     shutdownMarker = Config.applicationConf.getString("config.local.spark.shutdownPath")
     transactionDatasouce = Config.applicationConf.getString("config.local.spark.transaction.datasource")
     customerDatasource = Config.applicationConf.getString("config.local.spark.customer.datasource")
     modelPath = Config.applicationConf.getString("config.local.spark.model.path")
     preprocessingModelPath = Config.applicationConf.getString("config.local.spark.model.preprocessing.path")
     sparkConf.set("spark.sql.streaming.checkpointLocation", Config.applicationConf.getString("config.local.spark.checkpoint"))
       .set("spark.cassandra.connection.host", Config.applicationConf.getString("config.local.cassandra.host"))
   }

   def loadClusterConfig() = {

     shutdownMarker = Config.applicationConf.getString("config.cluster.spark.streaming.shutdownPath")
     transactionDatasouce = Config.applicationConf.getString("config.local.spark.transaction.datasource")
     customerDatasource = Config.applicationConf.getString("spark.local.customer.datasource")
     modelPath = Config.applicationConf.getString("spark.local.model.path")
     preprocessingModelPath = Config.applicationConf.getString("spark.local.model.preprocessing.path")
     sparkConf.set("spark.sql.streaming.checkpointLocation", Config.applicationConf.getString("config.cluster.spark.checkpoint"))
       .set("spark.cassandra.connection.host", Config.applicationConf.getString("config.cluster.cassandra.host"))

   }

  def setStandaloneMaster(master:String): Unit = {
    sparkConf.setMaster(master)
  }
 }

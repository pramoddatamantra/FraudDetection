package com.datamantra.config

import java.io.File

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.kafka.KafkaConfig
import com.datamantra.spark.SparkConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

/**
 * Created by kafka on 9/5/18.
 */
object Config {
  val logger = Logger.getLogger(getClass.getName)

  var applicationConf: Config = _


  var runMode = "local"
  var localProjectDir = ""

  /**
   * Parse a config object from command line inputs
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]) = {

    if(args.size == 0) {
      defaultSettiing
    } else {
      applicationConf = ConfigFactory.parseFile(new File(args(0)))
      val runMode = applicationConf.getString("config.mode")
      if(runMode == "local"){
        localProjectDir = s"file:///${System.getProperty("user.home")}/frauddetection/"
      }
      /*loadCommonConfig
      args(1) match {
        case "cluster" => loadClusterConfig()
        case _ => loadLocalConfig
      }
      */
      loadConfig()
    }

  }


  def loadConfig() = {

    CassandraConfig.load
    KafkaConfig.load
    SparkConfig.load
  }

  /*
  def loadCommonConfig() = {
    CassandraConfig.loadCommonConfig()
    KafkaConfig.loadCommonConfig()
    SparkConfig.loadCommonConfig()
  }

  def loadLocalConfig() = {
    CassandraConfig.loadLocalConfig()
    SparkConfig.loadLocalConfig()
    KafkaConfig.loadLocalConfig()
  }

  def loadClusterConfig() = {
    CassandraConfig.loadClusterConfig()
    KafkaConfig.loadClusterConfig()
    SparkConfig.loadClusterConfig()
  }
*/

  def defaultSettiing() = {

    CassandraConfig.defaultSettng()
    KafkaConfig.defaultSetting()
    SparkConfig.defaultSetting()
  }
}

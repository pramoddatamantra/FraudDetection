package com.datamantra.config

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.kafka.KafkaConfig
import com.datamantra.spark.SparkConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

/**
 * Created by kafka on 9/5/18.
 */
object Config {
  val logger = Logger.getLogger(getClass.getName)

  val applicationConf = ConfigFactory.load()

  var runMode = "local"

  /**
   * Parse a config object from command line inputs
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]) = {
    loadCommonConfig
    args(0) match {
      case "cluster" => loadClusterConfig()
      case _ => loadLocalConfig
    }

    /* This is a hack to run from intellij and standalone single node cluster */
    if(args(1) != null)
      SparkConfig.setStandaloneMaster(args(1))
  }


  def loadCommonConfig() = {
    SparkConfig.loadCommonConfig()
    KafkaConfig.loadCommonConfig()
    CassandraConfig.loadCommonConfig()
  }

  def loadLocalConfig() = {
    SparkConfig.loadLocalConfig()
    KafkaConfig.loadLocalConfig()
    CassandraConfig.loadLocalConfig()
  }

  def loadClusterConfig() = {
    SparkConfig.loadClusterConfig()
    KafkaConfig.loadClusterConfig()
    CassandraConfig.loadClusterConfig()
  }
}

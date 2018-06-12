package com.datamantra.kafka

import com.datamantra.config.Config

import scala.collection.mutable.Map

/**
 * Created by kafka on 21/5/18.
 */
object KafkaConfig {

  val kafkaParams: Map[String, String] = Map.empty

/*
  def loadCommonConfig() = {
    kafkaParams.put("topic", Config.applicationConf.getString("config.common.kafka.topic"))
    kafkaParams.put("enable.auto.commit", Config.applicationConf.getString("config.common.kafka.enable.auto.commit"))
    kafkaParams.put("group.id", Config.applicationConf.getString("config.common.kafka.group.id"))
  }
  def loadLocalConfig() = {
    kafkaParams.put("bootstrap", Config.applicationConf.getString("config.local.kafka.bootstrap.servers"))
  }

  def loadClusterConfig() = {

    kafkaParams.put("bootstrap", Config.applicationConf.getString("config.cluster.kafka.bootstrap.servers"))
  }
*/

  def load() = {
    println("Loading Kafka Setttings")
    kafkaParams.put("topic", Config.applicationConf.getString("config.kafka.topic"))
    kafkaParams.put("enable.auto.commit", Config.applicationConf.getString("config.kafka.enable.auto.commit"))
    kafkaParams.put("group.id", Config.applicationConf.getString("config.kafka.group.id"))
    kafkaParams.put("bootstrap.servers", Config.applicationConf.getString("config.kafka.bootstrap.servers"))
    kafkaParams.put("auto.offset.reset", Config.applicationConf.getString("config.kafka.auto.offset.reset"))
  }

  def defaultSetting() = {

    kafkaParams.put("topic", "creditcardTransaction")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("group.id", "RealTime Creditcard FraudDetection")
    kafkaParams.put("bootstrap.servers", "localhost:9092")
    kafkaParams.put("auto.offset.reset", "earliest")
  }

}

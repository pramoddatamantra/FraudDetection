package com.datamantra.kafka

import com.datamantra.config.Config

import scala.collection.mutable.Map

/**
 * Created by kafka on 21/5/18.
 */
object KafkaConfig {

  val kafkaParams: Map[String, String] = Map.empty


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


}

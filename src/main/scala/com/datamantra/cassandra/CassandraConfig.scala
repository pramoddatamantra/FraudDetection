package com.datamantra.cassandra

import com.datamantra.config.Config

/**
 * Created by kafka on 22/5/18.
 */
object CassandraConfig {

  var keyspace:String = _
  var table:String = _
  var cassandrHost:String = _


  def loadCommonConfig() = {
    keyspace = Config.applicationConf.getString("config.common.cassandra.keyspace")
    table = Config.applicationConf.getString("config.common.cassandra.table")
  }


  def loadClusterConfig() = {
    cassandrHost = Config.applicationConf.getString("config.cluster.cassandra.host")
  }


  def loadLocalConfig() = {
    cassandrHost = Config.applicationConf.getString("config.local.cassandra.host")
  }
}

package com.datamantra.cassandra

import com.datamantra.config.Config

/**
 * Created by kafka on 22/5/18.
 */
object CassandraConfig {

  var keyspace:String = _
  var transaction:String = _
  var customer:String = _
  var cassandrHost:String = _

  /*
  def loadCommonConfig() = {
    keyspace = Config.applicationConf.getString("config.common.cassandra.keyspace")
    transaction = Config.applicationConf.getString("config.common.cassandra.table.transaction")
    customer = Config.applicationConf.getString("config.common.cassandra.table.customer")
  }


  def loadClusterConfig() = {
    cassandrHost = Config.applicationConf.getString("config.cluster.cassandra.host")
  }


  def loadLocalConfig() = {
    cassandrHost = Config.applicationConf.getString("config.local.cassandra.host")
  }
*/

  def load() = {
    println("Loading Cassandra Setttings")
    keyspace = Config.applicationConf.getString("config.cassandra.keyspace")
    transaction = Config.applicationConf.getString("config.cassandra.table.transaction")
    customer = Config.applicationConf.getString("config.cassandra.table.customer")
    cassandrHost = Config.applicationConf.getString("config.cassandra.host")

  }

  def defaultSettng() = {
    keyspace = "creditcard"
    transaction = "transaction"
    customer = "customer"
    cassandrHost = "localhost"
  }
}

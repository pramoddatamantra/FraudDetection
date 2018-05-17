package com.datamantra.spark.jobs

import com.datamantra.spark.SparkHelper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by kafka on 9/5/18.
 */
abstract class SparkJob(appName:String) {

  val sparkConf = new SparkConf()
  sparkConf
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    .set("spark.cassandra.connection.host", "localhost")

  implicit val sparkSession = SparkSession.builder.
    master("local")
    .config(sparkConf)
    .appName("example")
    //.enableHiveSupport()
    .getOrCreate()
  //.set("spark.driver.memory", "2g")


  implicit var conf:Config = Config() // use default config if not overridden


}
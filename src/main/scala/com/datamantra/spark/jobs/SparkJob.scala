package com.datamantra.spark.jobs

import com.datamantra.config.Config
import com.datamantra.spark.SparkConfig
import com.datamantra.spark.SparkHelper._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by kafka on 9/5/18.
 */
abstract class SparkJob(appName:String) {

  implicit val sparkSession = SparkSession.builder
    .config(SparkConfig.sparkConf)
    .getOrCreate()

  var stopFlag:Boolean = false


}
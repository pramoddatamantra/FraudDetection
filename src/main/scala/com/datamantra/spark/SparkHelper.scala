package com.datamantra.spark

import com.datamantra.spark.jobs.SparkJob
import org.apache.spark.sql.SparkSession

/**
 * Created by kafka on 16/5/18.
 */
object SparkHelper extends SparkJob("Spark Helper"){

  def getSparkSession() = {
    sparkSession
  }
}

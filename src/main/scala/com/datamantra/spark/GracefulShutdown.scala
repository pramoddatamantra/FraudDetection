package com.datamantra.spark

import com.datamantra.spark.jobs.RealTimeFraudDetection.StructuredStreamingFraudDetection._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.StreamingContext

/**
 * Created by kafka on 10/6/18.
 */
object GracefulShutdown {

  def checkShutdownMarker = {
    if (!stopFlag) {
      //val fs = FileSystem.get(new Configuration())
      stopFlag =  new java.io.File(SparkConfig.shutdownMarker).exists()
    }

  }


  def struturedStreamingGracefulShutdown(checkIntervalMillis:Int, streamingQueries: List[StreamingQuery])(implicit sparkSession: SparkSession) {

    var isStopped = false

    while (! isStopped) {
      println("calling awaitTerminationOrTimeout")
      isStopped = sparkSession.streams.awaitAnyTermination(checkIntervalMillis)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running. Timeout...")
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        streamingQueries.map(query => {
          query.stop()
        })
        sparkSession.stop
        println("ssc is stopped!!!!!!!")
      }
    }
  }


  def dStreamGracefulShutdown(checkIntervalMillis:Int, ssc:StreamingContext)(implicit sparkSession: SparkSession)  {

    var isStopped = false

    while (! isStopped) {
      println("calling awaitTerminationOrTimeout")
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running. Timeout...")
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        ssc.stop(true, true)
        sparkSession.stop
        println("ssc is stopped!!!!!!!")
      }
    }

  }
}

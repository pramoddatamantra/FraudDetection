package com.datamantra.cassandra

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode

/**
 * Created by kafka on 14/5/18.
 */
object CassandraSink {

  def debugStream(ds: Dataset[_], mode: String = "append") = {

    val Outputmode = mode.toLowerCase match {
      case "complete" => OutputMode.Complete()
      case "append" => OutputMode.Append()
      case "update" => OutputMode.Update()
      case _ => OutputMode.Append()
    }


    ds.writeStream
      .format("console")
      .option("truncate", "false")
      .option("numRows", "100")
      .outputMode(mode).start()
  }
}
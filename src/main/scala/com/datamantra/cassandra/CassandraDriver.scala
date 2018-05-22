package com.datamantra.cassandra

import com.datamantra.cassandra.foreachSink.CassandraSinkForeach
import com.datamantra.config.Config
import com.datamantra.spark.SparkHelper
import com.datamantra.testing.Streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Created by kafka on 16/5/18.
 */
object CassandraDriver {

  val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

  def debugStream(ds: Dataset[_], mode: String = "append") {

    ds.writeStream
      .format("console")
      .option("truncate", "false")
      .option("numRows", "100")
      .outputMode(mode).start()
  }


  def saveForeach(df: DataFrame, db:String, table:String ) = {

    df
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .outputMode("append")
      .foreach(new CassandraSinkForeach(db, table))
      .start()
  }


  def readOffset(keyspace:String, table:String) = {

    val offsetDF = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace",keyspace)
      .option("table",table)
      .option("pushdown", "true")
      .load()
      .select("kafka_partition", "kafka_offset")
      .groupBy("kafka_partition").agg(max("kafka_offset") as "kafka_offset")

    if( offsetDF.rdd.isEmpty()) {
      ("startingOffsets", "earliest")
    }
    else {
      ("startingOffsets", transformKafkaMetadataArrayToJson(offsetDF.collect()))
    }
  }

  /**
   * @param array
   * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
   */
  def transformKafkaMetadataArrayToJson(array: Array[Row]) = {

    val partitionOffset = array
      .toList
      .foldLeft("")((a, i) => {
        a + s""""${i.getAs[Int](("kafka_partition"))}":${i.getAs[Long](("kafka_offset"))}, """
      })

    println("Offset: " + partitionOffset.substring(0, partitionOffset.size -2))

    s"""{"creditTransaction":
          {
           ${partitionOffset.substring(0, partitionOffset.size -2)}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
  }
 }

package com.datamantra.cassandra

import com.datamantra.cassandra.foreachSink.CassandraSinkForeach
import com.datamantra.spark.SparkConfig
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Created by kafka on 16/5/18.
 */
object CassandraDriver {

  //val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)
  val connector = CassandraConnector(SparkConfig.sparkConf)

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


  def readOffset(keyspace:String, table:String)(implicit sparkSession:SparkSession) = {

    import sparkSession.implicits._
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace",keyspace)
      .option("table",table)
      .option("pushdown", "true")
      .load()
      .select("partition", "offset")
      .filter($"partition".isNotNull)

    if( df.rdd.isEmpty()) {
      ("startingOffsets", "earliest")
    }
    else {
      val offsetDf = df.select("partition", "offset")
        .groupBy("partition").agg(max("offset") as "offset")
      ("startingOffsets", transformKafkaMetadataArrayToJson(offsetDf.collect()))
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
        a + s""""${i.getAs[Int](("partition"))}":${i.getAs[Long](("offset"))}, """
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

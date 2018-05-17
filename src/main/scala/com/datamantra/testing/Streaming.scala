package com.datamantra.testing

import org.apache.spark.sql.functions._
import com.datamantra.cassandra.CassandraDriver
import com.datamantra.kafka.KafkaSource
import com.datamantra.spark.jobs.SparkJob

/**
 * Created by kafka on 16/5/18.
 */
object Streaming extends SparkJob("Testing streaming Job"){

  /*
val transactionStructureName = "transaction"
val ransactionSchema = new StructType()
  .add("cc_num", StringType,true)
  .add("first", StringType, true)
  .add("last", StringType, true)
  .add("transactionId", StringType, true)
  .add("transactionDate", StringType, true)
  .add("transactionTime", StringType, true)
  .add("unixTime", StringType, true)
  .add("category", StringType, true)
  .add("merchant", StringType, true)
  .add("amt", DoubleType, true)
  .add("merchlat", DoubleType, true)
  .add("merchlong", DoubleType, true)
  .add("distance", DoubleType, true)
  .add("age", DoubleType, true)
  .add("is_fraud", BooleanType, true)
  .add("partition", IntegerType, true)
  .add("offset", LongType, true)
*/

  /*
    val ds = df.select("cc_num",
      "first",
      "last",
      "transactionId",
      "transactionDate",
      "transactionTime",
      "unixTime",
      "category",
      "merchant",
      "amt",
      "merchlat",
      "merchlong",
      "distance",
      "age",
      "prediction",
      "partition",
      "offset").as[FraudTransaction]
   */

  def readOffset(db:String, table:String) = {

    sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace","creditcard")
      .option("table","transaction")
      .load()
      .select("kafka_partition", "kafka_offset")
      .groupBy("kafka_partition").agg(max("kafka_offset"))
  }

  def main(args: Array[String]) {


    //val transactionDS = KafkaSource.readStream().select(KafkaSource.rawTransactionStructureName + ".*", "topic", "partition", "offset")

   // CassandraDriver.debugStream(transactionDS)

    //sparkSession.streams.awaitAnyTermination()

    val df = readOffset("creditcard", "transaction")

    df.show(false)
  }
}

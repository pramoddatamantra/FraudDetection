package com.datamantra.testing

import org.apache.spark.sql.Row
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

    val offsetDF = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace","creditcard")
      .option("table","transaction")
      .option("pushdown", "true")
      .load()
      .select("kafka_partition", "kafka_offset")
      .groupBy("kafka_partition").agg(max("kafka_offset") as "kafka_offset")

    if( offsetDF.rdd.isEmpty()) {
      ("startingOffsets", "earliest")
    }
    else {
      ("startingOffsets", transformKafkaMetadataArrayToJson1(offsetDF.collect()))
    }
  }



  /**
   * @param array
   * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
   */
  def transformKafkaMetadataArrayToJson(array: Array[Row]) : String = {
    s"""{"creditTransaction":
          {
           "${array(0).getAs[Int]("kafka_partition")}": ${array(0).getAs[Long]("kafka_offset")}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
  }


  /**
   * @param array
   * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
   */
  def transformKafkaMetadataArrayToJson1(array: Array[Row]) = {

    val partitionOffset = array
      .toList
      //.map(row => (row.getAs[Int](("kafka_partition")), row.getAs[Long](("kafka_offset"))))
      .foldLeft("")((a, i) => {
        a + s""""${i.getAs[Int](("kafka_partition"))}":${i.getAs[Long](("kafka_offset"))}, """
      })

    s"""{"creditTransaction":
          {
           ${partitionOffset.substring(0, partitionOffset.size -2)}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
  }

  def main(args: Array[String]) {


    //val transactionDS = KafkaSource.readStream().select(KafkaSource.rawTransactionStructureName + ".*", "topic", "partition", "offset")

   // CassandraDriver.debugStream(transactionDS)

    //sparkSession.streams.awaitAnyTermination()

    val offset = readOffset("creditcard", "transaction")

    println(offset)
  }
}

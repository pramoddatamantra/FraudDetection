package com.datamantra.testing

import java.net.InetAddress

import com.datamantra.config.Config
import com.datamantra.spark.SparkConfig
import org.apache.spark.sql.{SparkSession, Row}
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

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()


    import sparkSession.implicits._
    val df = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace","creditcard")
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


  def main(args: Array[String]) {


    //Config.parseArgs(args)
    //val transactionDS = KafkaSource.readStream().
     // select(KafkaSource.transactionStructureName + ".*", "topic", "partition", "offset")

   // CassandraDriver.debugStream(transactionDS)

    //sparkSession.streams.awaitAnyTermination()

    val offset = readOffset("creditcard", "transaction")
    println(offset)


  }
}

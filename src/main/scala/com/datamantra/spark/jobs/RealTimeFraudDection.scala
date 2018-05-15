package com.datamantra.spark.jobs


import java.io.InputStream
import java.util.Properties

import com.datamantra.cassandra.CassandraSink
import com.datamantra.spark.jobs.DataBalancing._
import com.datamantra.spark.pipeline.BuildPipeline
import com.datamantra.utils.Utils
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by kafka on 14/5/18.
 */
object RealTimeFraudDection extends SparkJob("Streaming Job to detect fraud transaction"){

  def main(args: Array[String]) {

     val input : InputStream = getClass.getResourceAsStream("/fraudDetection.properties")
    val prop: Properties = new Properties
    prop.load(input)

    val schema = StructType(
      Array(StructField("cc_num", StringType, true),
        StructField("first", StringType, true),
        StructField("last", StringType, true),
        StructField("transactionId", StringType, true),
        StructField("transactionDate", StringType, true),
        //StructField("transactionDate", TimestampType, true),
        StructField("transactionTime", StringType, true),
        //StructField("unixTime", LongType, true),
        StructField("unixTime", StringType, true),
        StructField("category", StringType, true),
        StructField("merchant", StringType, true),
        //StructField("amt", DoubleType, true),
        //StructField("merchlat", DoubleType, true),
        //StructField("merchlong", DoubleType, true)))
        StructField("amt", StringType, true),
        StructField("merchlat", StringType, true),
        StructField("merchlong", StringType, true)))


    import sparkSession.implicits._
    val transactionStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
      .option("subscribe", prop.getProperty("kafka.topic"))
      .option("startingOffsets", prop.getProperty("kafka.startingOffsets"))
      //.option("kafka.max.partition.fetch.bytes", prop.getProperty("kafka.max.partition.fetch.bytes"))
      //.option("kafka.max.poll.records", prop.getProperty("kafka.max.poll.records"))
      .load
      .selectExpr("CAST(value AS STRING) as message", "topic", "partition", "offset")
      .select(from_json(col("message"), schema).as("json"), $"topic", $"partition", $"offset")
      .select("json.*", "topic", "partition", "offset")
      .selectExpr("cc_num", "first", "last", "transactionId",
        "transactionDate", "transactionTime", "unixTime", "category",
        "merchant", "cast(amt as double) amt", "cast(merchlat as double) merch_lat", "cast(merchlong as double) merch_long",
        "topic", "partition", "offset")
      //.as[TrafficEvent]


    val readOption = Map("inferSchema" -> "true", "header" -> "true")

    val rawCustomerDF = sparkSession.read
      .options(readOption)
      .csv(conf.rawCustomerDataSource)
     rawCustomerDF.printSchema()

    val customer_age_df = rawCustomerDF
      .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
      .withColumnRenamed("cc_num", "cardNo")


    val distance_udf = udf(Utils.getDistance _)


    val processedTransactionDF = transactionStream.join(customer_age_df, customer_age_df("cardNo") === transactionStream("cc_num"))
      .withColumn("distance", lit(round(distance_udf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      //.select("cc_num" , "category", "merchant", "distance", "amt", "age")


    val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")



    val preprocessingModel = PipelineModel.load(conf.preprocessingModelPath)
    val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

    val randomForestModel = RandomForestClassificationModel.load(conf.modelPath)
    val predictionDF =  randomForestModel.transform(featureTransactionDF)



    CassandraSink.debugStream(predictionDF)

    sparkSession.streams.awaitAnyTermination()

  }
}

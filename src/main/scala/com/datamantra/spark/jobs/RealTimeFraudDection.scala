package com.datamantra.spark.jobs


import com.datamantra.cassandra.{CassandraConfig, CassandraDriver}
import com.datamantra.config.Config
import com.datamantra.kafka.KafkaSource
import com.datamantra.spark.SparkConfig
import com.datamantra.utils.Utils
import org.apache.spark.ml.{PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by kafka on 14/5/18.
 */
object RealTimeFraudDection extends SparkJob("Streaming Job to detect fraud transaction"){

  def checkShutdownMarker = {
    if (!stopFlag) {
      //val fs = FileSystem.get(new Configuration())
      stopFlag =  new java.io.File(SparkConfig.shutdownMarker).exists()
    }

  }

  def main(args: Array[String]) {

    Config.parseArgs(args)
    import sparkSession.implicits._

    val (startingOption, partitionsAndOffsets) = CassandraDriver.readOffset(CassandraConfig.keyspace, CassandraConfig.table)
    val transactionStream = KafkaSource.readStream(startingOption, partitionsAndOffsets)

     val readOption = Map("inferSchema" -> "true", "header" -> "true")
     val rawCustomerDF = sparkSession.read
      .options(readOption)
      .csv(SparkConfig.customerDatasource)
     rawCustomerDF.printSchema()

     val customer_age_df = rawCustomerDF
      .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
      .withColumnRenamed("cc_num", "cardNo")

     val distance_udf = udf(Utils.getDistance _)

     customer_age_df.cache()

     val processedTransactionDF = transactionStream.join(customer_age_df, customer_age_df("cardNo") === transactionStream("rawtransaction.cc_num"))
      .withColumn("distance", lit(round(distance_udf($"lat", $"long", $"rawtransaction.merchlat", $"rawtransaction.merchlong"), 2)))
      .selectExpr("rawTransaction.*", "distance", "age", "topic", "partition", "offset")
      .withColumn("amt", lit($"amt") cast(DoubleType))


     val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")


    val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
    val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

    val randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath)
    val predictionDF =  randomForestModel.transform(featureTransactionDF)


    CassandraDriver.saveForeach(predictionDF, CassandraConfig.keyspace, CassandraConfig.table)

    val checkIntervalMillis = 10000
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
        sparkSession.stop
        println("ssc is stopped!!!!!!!")
      }
    }

  }
}

package com.datamantra.spark.jobs


import java.io.InputStream
import java.util.Properties
import com.datamantra.cassandra.CassandraDriver
import com.datamantra.kafka.KafkaSource
import com.datamantra.utils.Utils
import org.apache.spark.ml.{PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassificationModel
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

    import sparkSession.implicits._

    val (startingOption, partitionsAndOffsets) = CassandraDriver.readOffset("creditcard", "transaction")
    val transactionStream = KafkaSource.readStream(startingOption, partitionsAndOffsets)

     val readOption = Map("inferSchema" -> "true", "header" -> "true")
     val rawCustomerDF = sparkSession.read
      .options(readOption)
      .csv(conf.rawCustomerDataSource)
     rawCustomerDF.printSchema()

     val customer_age_df = rawCustomerDF
      .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
      .withColumnRenamed("cc_num", "cardNo")

     val distance_udf = udf(Utils.getDistance _)


     val processedTransactionDF = transactionStream.join(customer_age_df, customer_age_df("cardNo") === transactionStream("rawtransaction.cc_num"))
      .withColumn("distance", lit(round(distance_udf($"lat", $"long", $"rawtransaction.merchlat", $"rawtransaction.merchlong"), 2)))
      .selectExpr("rawTransaction.*", "distance", "age", "topic", "partition", "offset")
      .withColumn("amt", lit($"amt") cast(DoubleType))


     val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")


    val preprocessingModel = PipelineModel.load(conf.preprocessingModelPath)
    val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

    val randomForestModel = RandomForestClassificationModel.load(conf.modelPath)
    val predictionDF =  randomForestModel.transform(featureTransactionDF)



    //CassandraDriver.debugStream(predictionDF)


    CassandraDriver.saveForeach(predictionDF, "creditcard", "transaction")


    sparkSession.streams.awaitAnyTermination()

  }
}

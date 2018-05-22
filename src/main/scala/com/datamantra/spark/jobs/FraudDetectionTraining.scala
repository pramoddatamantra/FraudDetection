package com.datamantra.spark.jobs

import com.datamantra.config.Config
import com.datamantra.spark.SparkConfig
import com.datamantra.spark.algorithms.Algorithms
import com.datamantra.spark.pipeline.BuildPipeline
import com.datamantra.utils.Utils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType


/**
 * Created by kafka on 9/5/18.
 */

object FraudDetectionTraining extends SparkJob("Balancing Fraud & Non-Fraud Dataset"){


  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._
    val readOption = Map("inferSchema" -> "true", "header" -> "true")

    val rawTransactionDF = sparkSession.read
      .options(readOption)
      .csv(SparkConfig.transactionDatasouce)
    rawTransactionDF.printSchema()


    val rawCustomerDF = sparkSession.read
      .options(readOption)
      .csv(SparkConfig.customerDatasource)
    rawCustomerDF.printSchema()


    val customer_age_df = rawCustomerDF
      .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
      .withColumnRenamed("cc_num", "cardNo")


    val distance_udf = udf(Utils.getDistance _)

    val processedTransactionDF = customer_age_df.join(rawTransactionDF, customer_age_df("cardNo") === rawTransactionDF("cc_num"))
      .withColumn("distance", lit(round(distance_udf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .selectExpr("cast(cc_num as string) cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    processedTransactionDF.cache()

    processedTransactionDF.show(false)


    val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")

    val pipelineStages = BuildPipeline.createFeaturePipeline(processedTransactionDF.schema, coloumnNames)

    val pipeline = new Pipeline().setStages(pipelineStages)

    val PreprocessingTransformerModel = pipeline.fit(processedTransactionDF)

    val featureDF = PreprocessingTransformerModel.transform(processedTransactionDF)

    PreprocessingTransformerModel.save(SparkConfig.preprocessingModelPath)

    val fraudFeatureDF = featureDF
      .filter($"is_fraud" === 1)
      .withColumnRenamed("is_fraud", "label")
      .select("features", "label")

    val nonFraudFeatureDF = featureDF.filter($"is_fraud" === 0)
    val fraudCount = fraudFeatureDF.count()

    featureDF.show(false)

    val kMeans = new KMeans().setK(fraudCount.toInt).setMaxIter(30)
    val kMeansModel = kMeans.fit(nonFraudFeatureDF)

    val featureSchema = StructType(
      Array(
        StructField("features", VectorType, true),
        StructField("label", IntegerType, true)
      ))

    val rowList = kMeansModel.clusterCenters.toList.map(v => Row(v, 0))
    val rowRdd = sparkSession.sparkContext.makeRDD(rowList)
    val sampledNonFraudFeatureDF = sparkSession.createDataFrame(rowRdd, featureSchema)


    val finalfeatureDF = fraudFeatureDF.union(sampledNonFraudFeatureDF)


    val randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF)

    randomForestModel.save(SparkConfig.modelPath)

  }

}

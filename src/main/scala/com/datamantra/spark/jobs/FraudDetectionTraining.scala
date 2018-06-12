package com.datamantra.spark.jobs

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.config.Config
import com.datamantra.spark.{DataBalancing, DataTransformation, SparkConfig}
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

    val fraudTransactionDF = DataTransformation.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val nonFraudTransactionDF = DataTransformation.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val transactionDF = nonFraudTransactionDF.union(fraudTransactionDF)
    transactionDF.cache()

    transactionDF.show(false)


    val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")

    val pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema, coloumnNames)
    val pipeline = new Pipeline().setStages(pipelineStages)
    val PreprocessingTransformerModel = pipeline.fit(transactionDF)
    PreprocessingTransformerModel.save(SparkConfig.preprocessingModelPath)

    val featureDF = PreprocessingTransformerModel.transform(transactionDF)


    val fraudDF = featureDF
      .filter($"is_fraud" === 1)
      .withColumnRenamed("is_fraud", "label")
      .select("features", "label")
    val nonFraudDF = featureDF.filter($"is_fraud" === 0)
    val fraudCount = fraudDF.count()

    featureDF.show(false)


    val balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.toInt)
    val finalfeatureDF = fraudDF.union(balancedNonFraudDF)


    val randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF)
    randomForestModel.save(SparkConfig.modelPath)

  }

}

package com.datamantra.spark.jobs

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.config.Config
import com.datamantra.creditcard.Schema
import com.datamantra.spark.algorithms.Algorithms
import com.datamantra.spark.algorithms.Algorithms.logger
import com.datamantra.spark.jobs.FraudDetectionTraining.sparkSession
import com.datamantra.spark.pipeline.BuildPipeline
import com.datamantra.spark.{DataBalancing, DataReader, SparkConfig}
import com.datamantra.utils.Utils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}


/**
  * Created by kafka on 9/5/18.
  */

object FraudDetectionTrainingTest extends SparkJob("Balancing Fraud & Non-Fraud Dataset"){


  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val fraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val nonFraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val transactionDF = nonFraudTransactionDF.union(fraudTransactionDF)
    transactionDF.cache()


    transactionDF.show(false)


    val coloumnNames = List("category", "merchant", "distance", "amt", "age")


    /*Transform raw numberical columns to vector. Slice the vecotor and Scale the vector.  Scaling is required so that all the column values are at the same level of measurement */
    val numericAssembler = new VectorAssembler().setInputCols(Array("distance", "amt", "age" )).setOutputCol("rawfeature")
    val slicer = new VectorSlicer().setInputCol("rawfeature").setOutputCol("slicedfeatures").setNames(Array("distance", "amt", "age"))
    val scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures")



    /*String Indexer for category and merchant columns*/
    val categoryIndexer = new StringIndexer().setInputCol("category").setOutputCol("category_indexed")
    val merchantIndexer = new StringIndexer().setInputCol("merchant").setOutputCol("merchant_indexed")


    /*val categoryOneHotEncoder = new OneHotEncoder().setInputCol("category_indexed").setOutputCol("category_encoded")
    val merchantOneHotEncoder = new OneHotEncoder().setInputCol("merchant_indexed").setOutputCol("merchant_encoded")*/


    /*Transform all the required columns as feature vector*/
    //val vectorAssember = new VectorAssembler().setInputCols(Array("scaledfeatures")).setOutputCol("features")
    val vectorAssember = new VectorAssembler().setInputCols(Array("category_indexed", "merchant_indexed", "scaledfeatures")).setOutputCol("features")


    //val pipeline = new Pipeline().setStages(Array(categoryIndexer, merchantIndexer, categoryOneHotEncoder, merchantOneHotEncoder, numericAssembler, slicer, scaler, vectorAssember))
    val pipeline = new Pipeline().setStages(Array(categoryIndexer, merchantIndexer, numericAssembler, slicer, scaler, vectorAssember))
    val preprocessingTransformerModel = pipeline.fit(transactionDF)
    val featureDF = preprocessingTransformerModel.transform(transactionDF)
    featureDF.show(false)


    val Array(trainData, testData) = featureDF.randomSplit(Array(0.8, 0.2))

    val featureLabelDF = trainData.select("features", "is_fraud").cache()

    val nonFraudDF = featureLabelDF.filter($"is_fraud" === 0)


    val fraudDF = featureLabelDF.filter($"is_fraud" === 1)
    val fraudCount = fraudDF.count()


    println("fraudCount: " + fraudCount)


    /* There will be very few fraud transaction and more normal transaction. Models created  from such
     * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
     */
    val balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.toInt)

    val finalfeatureDF = fraudDF.union(balancedNonFraudDF)



    val randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF)
    val predictionDF = randomForestModel.transform(testData)
    predictionDF.show(false)


    val predictionAndLabels =
      predictionDF.select(col("prediction"), col("is_fraud").cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }.cache()


    val tp = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 1 }.count().toFloat
    val fp = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 1 }.count().toFloat
    val tn = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 0 }.count().toFloat
    val fn = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 0 }.count().toFloat



    printf(s"""|=================== Confusion matrix ==========================
               |#############| %-15s                     %-15s
               |-------------+-------------------------------------------------
               |Predicted = 1| %-15f                     %-15f
               |Predicted = 0| %-15f                     %-15f
               |===============================================================
         """.stripMargin, "Actual = 1", "Actual = 0", tp, fp, fn, tn)


    println()



    val metrics =new MulticlassMetrics(predictionAndLabels)

    /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
    println("True Positive Rate: " + tp/(tp + fn))  // tp/(tp + fn)

    /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
    println("False Positive Rate: " + fp/(fp + tn))

    println("Precision: " +  tp/(tp + fp))

  }

}

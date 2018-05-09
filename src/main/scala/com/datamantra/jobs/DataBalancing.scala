package com.datamantra.jobs

import com.datamantra.pipeline.{FeatureExtraction, BuildPipeline}
import org.apache.spark.ml.{Transformer, Estimator, Pipeline}
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, DataFrame}


/**
 * Created by kafka on 9/5/18.
 */

object DataBalancing extends SparkJob("Balancing Fraud & Non-Fraud Dataset"){


  def getDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
    val r : Int = 6371 //Earth radius
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(lon2 - lon1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }



  /*
  def banlanceDataSet(df:DataFrame, labelToBalance: Int, numOfClusters: Int)(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val filteredDFWithMoreLables = df.filter($"is_fraud" === labelToBalance)
    val filteredDfWithLessLabels = df.filter($"is_fraud" === 1)
    val TransactionWithFeatures = vectorAssemblerForSampling(filteredDFWithMoreLables)
    val kMeans = new KMeans().setK(numOfClusters).setMaxIter(30)
    TransactionWithFeatures.show()
    val data = df.cache()
    val model = kMeans.fit(TransactionWithFeatures)
    val clusterCentres = model.clusterCenters
    //    for(i<-clusterCentres)
    //      println(i)
    val transactionDFAfterSampling = sparkSession.sparkContext.parallelize(clusterCentres).map(x => (x, 0)).map(x => Utils.vectorToRDD(x)).toDF()
    vectorAssembler(filteredDfWithLessLabels).unionAll(vectorAssembler(transactionDFAfterSampling))

  }


  def isImbalance(df:DataFrame)(implicit sparkSession:SparkSession, config: Config) = {
    import sparkSession.implicits._
    val numOfPositiveRecords = df.filter($"is_fraud" === 0).count().toInt
    val positiveLabel = 0
    val numofNegetiveRecords = df.count().toInt - numOfPositiveRecords.toInt
    val negativeLabel = 1
    val totalPoints = numofNegetiveRecords + numOfPositiveRecords
    var numOFClusters = 0
    if ((numOfPositiveRecords / totalPoints) * 100 < 30){  //(true,whichLabeltoReduce,numofClusters
      println("I am in pos")

      (true, positiveLabel, numofNegetiveRecords)
    } else if (numofNegetiveRecords / totalPoints.toInt * 100 < 30) {
      println(" iam in Negaitev part")
      (true, negativeLabel, numOfPositiveRecords)
    } else
      (false, 0, 0)
  }

*/
  def main(args: Array[String]) {


    import sparkSession.implicits._
    val readOption = Map("inferSchema" -> "true", "header" -> "true")
    //cc_num,first,last,trans_num,trans_date,trans_time,unix_time,category,merchant,amt,merch_lat,merch_long



    val rawTransactionDF = sparkSession.read
      .options(readOption)
      .csv(conf.rawTransactionDataSource)
    rawTransactionDF.printSchema()


    val rawCustomerDF = sparkSession.read
      .options(readOption)
      .csv(conf.rawCustomerDataSource)
    rawCustomerDF.printSchema()


    val customer_age_df = rawCustomerDF
      .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
      .withColumnRenamed("cc_num", "cardNo")


    val distance_udf = udf(getDistance _)

    val processedTransactionDF = customer_age_df.join(rawTransactionDF, customer_age_df("cardNo") === rawTransactionDF("cc_num"))
      .withColumn("distance", lit(round(distance_udf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .selectExpr("cast(cc_num as string) cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    processedTransactionDF.cache()

    val fraudDF = processedTransactionDF.filter($"is_fraud" === 1)
    val nonFraudDF = processedTransactionDF.filter($"is_fraud" === 0)

    val fraudCount = fraudDF.count()

    val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")

    var pipelineStages = BuildPipeline.createFeaturePipeline(processedTransactionDF.schema, coloumnNames)



    val nonFraudFeatureDF  = pipelineStages.foldLeft(nonFraudDF)((df, stage) => {

      stage match {
        case o: OneHotEncoder => o.transform(df)
        case s: StringIndexer => s.fit(df).transform(df)
        case v: VectorAssembler => v.transform(df)
      }
    })

    nonFraudFeatureDF.show(false)


    val kMeans = new KMeans().setK(fraudCount.toInt).setMaxIter(30)

    val model = kMeans.fit(nonFraudFeatureDF)
    val clusterCentres = model.clusterCenters

    clusterCentres.foreach(v => println(v.size))












    /*

    val fraudDF = df.filter($"is_fraud" === 1)
    val nonFraudDF = df.filter($"is_fraud" === 0)



    val kMeans = new KMeans().setK(fraudCount.toInt).setMaxIter(30)

    val model = kMeans.fit(nonFraudDF)

*/

  }

}

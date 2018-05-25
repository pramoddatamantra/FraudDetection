package com.datamantra.spark.algorithms

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession

/**
 * Created by kafka on 15/5/18.
 */
object Algorithms {

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession) = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionwithPrediction = model.transform(test)
    transactionwithPrediction.show(false)
    println(s"total data count is" + transactionwithPrediction.count())
    println("count of same label " + transactionwithPrediction.filter($"prediction" === $"label").count())
    model
  }


  /* def logisticRegressionClassifier(df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    df.cache()
    val Array(spark.training, test) = df.randomSplit(Array(0.7, 0.3))
    val logisticEstimator = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
    val model = logisticEstimator.fit(spark.training)
    val transactionwithPrediction = model.transform(test)
    println(s"total data count is"+transactionwithPrediction.count())
    println("count of same label "+transactionwithPrediction.filter($"prediction" === $"label").count())
    model
  }*/
}

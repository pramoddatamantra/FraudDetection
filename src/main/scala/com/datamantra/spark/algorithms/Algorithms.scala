package com.datamantra.spark.algorithms

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
  * Created by kafka on 15/5/18.
  */
object Algorithms {

  val logger = Logger.getLogger(getClass.getName)

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession) = {
    import sparkSession.implicits._
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("is_fraud").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(df)
    model
  }
}

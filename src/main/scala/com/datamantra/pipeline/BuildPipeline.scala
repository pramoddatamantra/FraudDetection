package com.datamantra.pipeline

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

import scala.collection.mutable

/**
 * Created by kafka on 9/5/18.
 */
object BuildPipeline {

  def createFeaturePipeline(schema:StructType, columns:List[String]):Array[PipelineStage] = {
    val featureColumns = mutable.ArrayBuffer[String]()
    val preprocessingStages = schema.fields.filter(field => columns.contains(field.name)).flatMap(field => {

      field.dataType match {
        case s: StringType => {

          val stringIndexer = new StringIndexer()
          stringIndexer.setInputCol(field.name).setOutputCol(s"${field.name}_indexed")

          val oneHotEncoder = new OneHotEncoder()
          oneHotEncoder.setInputCol(s"${field.name}_indexed").setOutputCol(s"${field.name}_encoded")

          featureColumns += (s"${field.name}_encoded")
          Array[PipelineStage](stringIndexer, oneHotEncoder)
        }

        case n: NumericType => {
          featureColumns += (field.name)
          Array.empty[PipelineStage]
        }
        case _ => {
          Array.empty[PipelineStage]
        }
      }

    })

    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(featureColumns.toArray).setOutputCol("features")

    (preprocessingStages :+ vectorAssembler)

  }
}

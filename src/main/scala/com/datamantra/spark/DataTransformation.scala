package com.datamantra.spark

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.creditcard.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SaveMode, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType}
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/**
 * Created by kafka on 24/5/18.
 */
object DataTransformation {

  def read(transactionDatasource:String, schema:StructType)(implicit sparkSession:SparkSession) = {
    sparkSession.read
      .option("header", "true")
      .schema(schema)
      .csv(transactionDatasource)
  }


  def saveToCassandra(ds: Dataset[_], keySpace:String, table:String, mode:String) = {

    val savemode = mode.toLowerCase match {
      case "append" => SaveMode.Append
      case "Overwrite" => SaveMode.Overwrite
      case "ErrorIfExists" => SaveMode.ErrorIfExists
      case "Ignore" => SaveMode.Ignore
    }

    ds.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table))
      .mode(savemode)
      .save()
  }


  def readFromCassandra(keySpace:String, table:String)(implicit sparkSession:SparkSession) = {

    sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table, "pushdown" -> "true"))
      .load()
  }


  def getOffset(rdd: RDD[_])(implicit sparkSession:SparkSession) = {

    import  sparkSession.implicits._
    rdd.asInstanceOf[HasOffsetRanges]
      .offsetRanges.toList
      .map(offset => (offset.partition, offset.untilOffset))
      .toDF("partition", "offset")
  }
}

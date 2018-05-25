package com.datamantra.spark

import com.datamantra.cassandra.CassandraConfig
import com.datamantra.creditcard.Schema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType}

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


  def saveToCassandra(ds: Dataset[_], keySpace:String, table:String) = {

    ds.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table))
      .save()
  }


  def readFromCassandra(keySpace:String, table:String)(implicit sparkSession:SparkSession) = {

    sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table, "pushdown" -> "true"))
      .load()
  }

}

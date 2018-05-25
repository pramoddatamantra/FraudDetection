package com.datamantra.spark.jobs

import com.datamantra.cassandra.{CassandraConfig, CassandraDriver}
import com.datamantra.config.Config
import com.datamantra.creditcard.Schema
import com.datamantra.spark.{SparkConfig, DataTransformation}
import com.datamantra.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Created by kafka on 24/5/18.
 */
object IntialImportToCassandra extends SparkJob("Initial Import to Cassandra"){

  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val transactionDF = DataTransformation.read(SparkConfig.transactionDatasouce, Schema.fruadCheckedTransactionSchema)

    val customerDF = DataTransformation.read(SparkConfig.customerDatasource, Schema.customerSchema)

        /* Save Customer data to cassandra */
    DataTransformation.saveToCassandra(customerDF, CassandraConfig.keyspace, CassandraConfig.customer)


    val customerAgeDF = customerDF.withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))


    val distanceUdf = udf(Utils.getDistance _)

    val processedDF = transactionDF.join(customerAgeDF, Seq("cc_num"))
      .withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .select("cc_num", "trans_num", "trans_date", "trans_time", "unix_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")

    processedDF.printSchema()
    processedDF.show

    /* Save transformeed Transaction data to cassandra */
    DataTransformation.saveToCassandra(processedDF, CassandraConfig.keyspace, CassandraConfig.transaction)

  }

}

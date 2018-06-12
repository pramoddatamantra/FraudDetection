package com.datamantra.spark.jobs.RealTimeFraudDetection

import com.datamantra.cassandra.dao.{KafkaOffsetRepository, CreditcardTransactionRepository}
import com.datamantra.cassandra.{CassandraDriver, CassandraConfig}
import com.datamantra.config.Config
import com.datamantra.creditcard.Schema
import com.datamantra.kafka.KafkaConfig
import com.datamantra.spark.{GracefulShutdown, SparkConfig, DataTransformation}
import com.datamantra.spark.jobs.SparkJob
import com.datamantra.utils.Utils
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType, DoubleType, IntegerType}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import com.datastax.spark.connector.streaming._

import scala.collection.mutable.Map

/**
 * Created by kafka on 9/6/18.
 */
object DstreamFraudDetection extends SparkJob("Fraud Detection using Dstream"){

  def main (args: Array[String]){

    Config.parseArgs(args)

    import sparkSession.implicits._
    val customerDF = DataTransformation.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.customer)
    val customerAgeDF = customerDF.withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
    customerAgeDF.cache()

    val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
    val randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath)

    /*
       Connector Object is created in driver. It is serializable.
       So once the executor get it, they establish the real connection
    */
    val connector = CassandraConnector(sparkSession.sparkContext.getConf)

    val ssc = new StreamingContext(sparkSession.sparkContext, Duration(SparkConfig.batchInterval))


    val topics = Set(KafkaConfig.kafkaParams("topic"))
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KafkaConfig.kafkaParams("bootstrap.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> KafkaConfig.kafkaParams("group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> KafkaConfig.kafkaParams("auto.offset.reset")
    )


    val storedOffsets = CassandraDriver.readOffset(CassandraConfig.keyspace,
                           CassandraConfig.kafkaOffsetTable, KafkaConfig.kafkaParams("topic"))

    val stream = storedOffsets match {
      case None => {
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topics, kafkaParams)
                       )
      }

      case Some(fromOffsets) => {
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
      }
    }

    val transactionStream =  stream.map(cr => (cr.value(), cr.partition(), cr.offset()))

    transactionStream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {

        val kafkaTransactionDF = rdd.toDF("transaction", "partition", "offset")
          .withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
            from_json($"transaction".cast(StringType), Schema.kafkaTransactionSchema)) //From binary to JSON object
          .select("transaction.*", "partition", "offset")
          .withColumn("amt", lit($"amt") cast (DoubleType))
          .withColumn("merch_lat", lit($"merch_lat") cast (DoubleType))
          .withColumn("merch_long", lit($"merch_long") cast (DoubleType))
          .withColumn("trans_time", lit($"trans_time") cast (TimestampType))
          .drop("first")
          .drop("last")



        val distanceUdf = udf(Utils.getDistance _)

        sparkSession.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
        val processedTransactionDF = kafkaTransactionDF.join(broadcast(customerAgeDF), Seq("cc_num"))
          .withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
          .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "partition", "offset")


        val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)
        val predictionDF = randomForestModel.transform(featureTransactionDF)
          .withColumnRenamed("prediction", "is_fraud")
          .select("cc_num", "trans_time", "trans_num", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud", "partition", "offset")

        predictionDF.printSchema()
        predictionDF.show(false)

        /*
         Connector Object is created in driver. It is serializable.
         It is serialized and send to executor. Once the executor get it, they establish the real connection
        */
        predictionDF.foreachPartition(partitionOfRecords => {

          /*
          Writing to Fraud, NonFruad and Offset Table in single iteration
          Cassandra prepare statement is used because it avoids pasring of the column for every instert and hence efficient
          Offset is inserted last to achieve atleast once semantics. it is possible that it may read duplicate creditcard
          transactions from kafka while restart.
          Even though duplicate creditcard transaction are read from kafka, writing to Cassandra is idempotent. Becasue
          cc_num and trans_time is the primary key. So you cannot have duplicate records with same cc_num and trans_time.
          As a result we achive exactly once semantics.
         */
          connector.withSessionDo(session => {
            //Prepare Statment for all three tables
            val preparedStatementFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable))
            val preparedStatementNonFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable))
            val preparedStatementOffset = session.prepare(KafkaOffsetRepository.cqlOffsetPrepare(CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable))

            val partitionOffset:Map[Int, Long] = Map.empty
            partitionOfRecords.foreach(record => {
              val isFraud = record.getAs[Double]("is_fraud")
              println("isFraud: " + isFraud)
              if (isFraud == 1.0) {
                // Bind and execute prepared statement for Fraud Table
                session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementFraud, record))
              }
              else if(isFraud == 0.0) {
                // Bind and execute prepared statement for NonFraud Table
                session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementNonFraud, record))
              }
              //Get max offset in the current match
              val kafkaPartition = record.getAs[Int]("partition")
              val offset = record.getAs[Long]("offset")
              partitionOffset.get(kafkaPartition) match  {
                case None => partitionOffset.put(kafkaPartition, offset)
                case Some(currentMaxOffset) => {
                  if(offset > currentMaxOffset)
                    partitionOffset.update(kafkaPartition, offset)
                }

              }

            })
            partitionOffset.foreach(t => {
              // Bind and execute prepared statement for Offset Table
              session.execute(KafkaOffsetRepository.cqlOffsetBind(preparedStatementOffset, t))

            })

          })
        })

      }
      else {
        println("Did not receive any data")
      }

    })

    ssc.start()
    GracefulShutdown.dStreamGracefulShutdown(1000, ssc)
  }

}

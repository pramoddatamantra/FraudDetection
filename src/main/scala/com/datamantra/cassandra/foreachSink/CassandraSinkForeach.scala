package com.datamantra.cassandra.foreachSink


import com.datamantra.cassandra.CassandraDriver
import com.datamantra.creditcard.FraudTransaction
import org.apache.spark.sql.{Row, ForeachWriter}

/**
 * Created by kafka on 16/5/18.
 */


class CassandraSinkForeach(db:String, table:String) extends ForeachWriter[Row] {
  private def cqlTransaction(record: Row): String = s"""
     insert into $db.$table (
       cc_num,
       first,
       last,
       transactionId,
       transactionDate,
       transactionTime,
       unixTime,
       category,
       merchant,
       amt,
       merchlat,
       merchlong,
       distance,
       age,
       is_fraud,
       kafka_partition,
       kafka_offset
     )
     values(
       '${record.getAs[String]("cc_num")  }',
       '${record.getAs[String]("first")}',
       '${record.getAs[String]("last")}',
       '${record.getAs[String]("transactionId")}',
       '${record.getAs[String]("transactionDate")}',
       '${record.getAs[String]("transactionTime")}',
       '${record.getAs[String]("unixTime")}',
       '${record.getAs[String]("category")}',
       '${record.getAs[String]("merchant")}',
        ${record.getAs[Double]("amt")},
        ${record.getAs[Double]("merchlat")},
        ${record.getAs[Double]("merchlong")},
        ${record.getAs[Double]("distance")},
        ${record.getAs[Double]("age")},
        ${record.getAs[Double]("prediction")},
        ${record.getAs[Int]("partition")},
        ${record.getAs[Long]("offset")}
        )"""

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    //@TODO command to check if cassandra cluster is up
    true
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md#connection-pooling
  def process(record: Row) = {
    println(s"Saving record: $record")
    CassandraDriver.connector.withSessionDo(session =>
      session.execute(cqlTransaction(record))
    )
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-connection-parameters

  def close(errorOrNull: Throwable): Unit = {

    //CassandraDriver.connector.withClusterDo(session => session.close())
    // close the connection
    //connection.keep_alive_ms	--> 5000ms :	Period of time to keep unused connections open
  }
}

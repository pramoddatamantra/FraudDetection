package com.datamantra.cassandra.foreachSink


import com.datamantra.cassandra.CassandraDriver
import com.datamantra.creditcard.Enums
import org.apache.spark.sql.{Row, ForeachWriter}

/**
 * Created by kafka on 16/5/18.
 */


class CassandraSinkForeach(db:String, table:String) extends ForeachWriter[Row] {
  private def cqlTransaction(record: Row): String = s"""
     insert into $db.$table (
       ${Enums.TransactionCassandra.cc_num},
       ${Enums.TransactionCassandra.trans_num},
       ${Enums.TransactionCassandra.trans_date},
       ${Enums.TransactionCassandra.trans_time},
       ${Enums.TransactionCassandra.unix_time},
       ${Enums.TransactionCassandra.category},
       ${Enums.TransactionCassandra.merchant},
       ${Enums.TransactionCassandra.amt},
       ${Enums.TransactionCassandra.merch_lat},
       ${Enums.TransactionCassandra.merch_long},
       ${Enums.TransactionCassandra.distance},
       ${Enums.TransactionCassandra.age},
       ${Enums.TransactionCassandra.is_fraud},
       ${Enums.TransactionCassandra.kafka_partition},
       ${Enums.TransactionCassandra.kafka_offset}
     )
     values(
       '${record.getAs[String](Enums.TransactionCassandra.cc_num)  }',
       '${record.getAs[String](Enums.TransactionCassandra.trans_num)}',
       '${record.getAs[String](Enums.TransactionCassandra.trans_date)}',
       '${record.getAs[String](Enums.TransactionCassandra.trans_time)}',
       '${record.getAs[String](Enums.TransactionCassandra.unix_time)}',
       '${record.getAs[String](Enums.TransactionCassandra.category)}',
       '${record.getAs[String](Enums.TransactionCassandra.merchant)}',
        ${record.getAs[Double](Enums.TransactionCassandra.amt)},
        ${record.getAs[Double](Enums.TransactionCassandra.merch_lat)},
        ${record.getAs[Double](Enums.TransactionCassandra.merch_long)},
        ${record.getAs[Double](Enums.TransactionCassandra.distance)},
        ${record.getAs[Double](Enums.TransactionCassandra.age)},
        ${record.getAs[Double](Enums.TransactionCassandra.is_fraud)},
        ${record.getAs[Int](Enums.TransactionCassandra.kafka_partition)},
        ${record.getAs[Long](Enums.TransactionCassandra.kafka_offset)}
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

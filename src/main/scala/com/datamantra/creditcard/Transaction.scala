package com.datamantra.creditcard

import java.sql.Timestamp

/**
 * Created by kafka on 16/5/18.
 */

case class Transaction(cc_num:String,
                       first:String,
                       last:String,
                       trans_num:String,
                       trans_time: Timestamp,
                       //unix_time:String,
                       category:String,
                       merchant:String,
                       amt:String,
                       merch_lat: String,
                       merch_long:String)

case class DstreamTransaction(cc_num:String,
                       first:String,
                       last:String,
                       trans_num:String,
                       trans_time: Timestamp,
                       //unix_time:String,
                       category:String,
                       merchant:String,
                       amt:Double,
                       merch_lat: Double,
                       merch_long:Double)

case class FraudTransaction(cc_num:String,
                            first:String,
                            last:String,
                            transactionId:String,
                            transactionTime: String,
                            category:String,
                            merchant:String,
                            amt:Double,
                            merchlat: Double,
                            merchlong:Double,
                            distance:Double,
                            age:Int,
                            prediction: Boolean,
                            partition:Int,
                            offset: Long)

case class TransactionKafka(topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType:Int, transaction:Transaction)



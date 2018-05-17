package com.datamantra.creditcard

import java.sql.Timestamp

/**
 * Created by kafka on 16/5/18.
 */

case class RawTransaction(cc_num:String,
                       first:String,
                       last:String,
                       transactionId:String,
                       transactionDate: String,
                       transactionTime: String,
                       unixTime: String,
                       category:String,
                       merchant:String,
                       amt:String,
                       merchlat: String,
                       merchlong:String)

case class FraudTransaction(cc_num:String,
                            first:String,
                            last:String,
                            transactionId:String,
                            transactionDate: String,
                            transactionTime: String,
                            unixTime: Long,
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

case class TransactionKafka(topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType:Int, rawtransaction:RawTransaction)



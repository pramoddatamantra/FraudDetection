package com.datamantra.pipeline.entities

/**
 * Created by kafka on 9/5/18.
 */
case class Transaction(cc_num: String, category: String, merchant: String, distance: Double, amt: Double, age:Int, is_fraud: Int)


package com.datamantra.spark.jobs

import org.apache.log4j.Logger

/**
 * Created by kafka on 9/5/18.
 */
case class Config
( rawTransactionDataSource:String = "/home/kafka/Downloads/Training/data/creditcard/transaction/output/transactions.csv",
  rawCustomerDataSource:String = "/home/kafka/Downloads/Training/data/creditcard/customer/output/customer.csv",
  date_ts: String = "some-hard-coded-default-value",
  runMode: String = "prod"
  )

object Config {
  val logger = Logger.getLogger(getClass.getName)

  /**
   * Parse a config object from command line inputs
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]): Config = {
    //    val outputFormats = List("csv", "orc", "parquet")
    //    val sparkSaveModes = List("append", "overwrite", "ErrorIfExists", "Ignore")
    val runModes = List("prod", "data-debug")

    // Generating the parser with all validations
    val parser = new scopt.OptionParser[Config]("spark-submit --class" +
      " <Class-Name> <Jar-Path>") {
      head("ETL on Spark", "v0.1")

      opt[String]("date_ts") required() action {
        (x, c) => c.copy(date_ts = x)
      } text "TimeStamp when the ETL is running"

      opt[String]("runMode") validate {
        x => if (runModes.contains(x)) success
        else failure(s"Invalid runMode. Currently supported $runModes")
      } action {
        (x, c) => c.copy(runMode = x)
      } text "use 'data-debug' to print intermediate data frames to logs. Defaults to 'prod'"

      help("help") text "prints this usage text"
    }

    parser.parse(args, Config()) match {
      case Some(c) => logger.info("Config for the app is " + c.toString)
        c
      case None => throw new IllegalArgumentException("Invalid config")
    }
  }

}

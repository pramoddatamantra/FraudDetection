package com.datamantra.kafka

import org.apache.spark.sql.DataFrame

/**
 * Created by kafka on 9/6/18.
 */
object KafkaSink {


  def saveStream(df:DataFrame, topic:String) = {

    df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConfig.kafkaParams("bootstrap"))
      .option("topic", topic)
      .start()
  }
}

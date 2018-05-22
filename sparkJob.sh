#!/usr/bin/env bash
Streaming Spark Job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --class com.datamantra.spark.jobs.RealTimeFraudDection  $HOME/frauddetection/spark/realtime-fruaddetection-spark.jar $HOME/frauddetection/application.conf local


Training Spark Job
spark-submit --class com.datamantra.spark.jobs.FraudDetectionTraining  $HOME/frauddetection/spark/realtime-fruaddetection-spark.jar  $HOME/frauddetection/application.conf local

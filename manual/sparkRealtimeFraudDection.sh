#!/usr/bin/env bash
#Streaming Spark Job
spark-submit --class com.datamantra.spark.jobs.RealTimeFraudDection --name "RealTime Creditcard FraudDetection" --master spark://datamantra:7077 --deploy-mode cluster  $HOME/frauddetection/spark/fruaddetection-spark.jar $HOME/frauddetection/spark/application-local.conf

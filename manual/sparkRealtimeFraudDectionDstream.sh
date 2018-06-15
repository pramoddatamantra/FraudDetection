#!/usr/bin/env bash
#Dstream Streaming Spark Job
spark-submit --class com.datamantra.spark.jobs.RealTimeFraudDetection.DstreamFraudDetection --name "RealTime Creditcard FraudDetection Dstream" --master spark://datamantra:7077 --deploy-mode client  $HOME/frauddetection/spark/fruaddetection-spark.jar $HOME/frauddetection/spark/application-local.conf

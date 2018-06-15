#!/usr/bin/env bash

rm -rf build-files && \
rm -rf $HOME/frauddetection
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to Local" && \
mkdir -p $HOME/frauddetection && \
cp -r build-files/* $HOME/frauddetection && \
echo "Deployed in Local"

exit 0

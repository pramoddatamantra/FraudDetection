#!/usr/bin/env bash

destIp=ec2-13-126-76-243.ap-south-1.compute.amazonaws.com

rm -rf build-files && \
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to $destIp" && \
scp -i 'cloudera.pem' -r build-files/* ubuntu@$destIp:codeDrops/ && \
echo "Deployed in VM $destIp"

exit 0



scp -i 'cloudera.pem' demo-data/omniture-raw/* ubuntu@destIp:dataDrops/
scp -i 'cloudera.pem' demo-data/sales-raw/* ubuntu@destIp:dataDrops/


cd codeDrops/local
sh local/05_IngestData.sh  ~/dataDrops/Omniture.0.tsv.gz ~/dataDrops/online-retail-sales-data.csv


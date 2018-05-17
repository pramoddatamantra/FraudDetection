CREATE KEYSPACE IF NOT EXISTS creditcard
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

USE creditcard;

CREATE TABLE IF NOT EXISTS transaction (
  cc_num text,
  first text,
  last text,
  transactionId text,
  transactionDate text,
  transactionTime text,
  unixTime text,
  category text,
  merchant text,
  amt double,
  merchlat double,
  merchlong double,
  distance double,
  age int,
  is_fraud double,
  kafka_partition int,
  kafka_offset bigint,
  PRIMARY KEY(cc_num, transactionId));
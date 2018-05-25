CREATE KEYSPACE IF NOT EXISTS creditcard
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

USE creditcard;

CREATE TABLE IF NOT EXISTS transaction (
  cc_num text,
  trans_num text,
  trans_date timestamp,
  trans_time text,
  unix_time text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  age int,
  is_fraud double,
  partition int,
  offset bigint,
  PRIMARY KEY(cc_num, trans_num)
);


CREATE TABLE IF NOT EXISTS customer (
  cc_num text,
  first text,
  last text,
  gender text,
  street text,
  city text,
  state text,
  zip text,
  lat double,
  long double,
  job text,
  dob timestamp,
  PRIMARY KEY(cc_num)
);

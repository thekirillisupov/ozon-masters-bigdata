#hive script
CREATE DATABASE IF NOT EXISTS thekirillisupov;

USE thekirillisupov;

CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS  thekirillisupov.hw2_test
(  id INT,
   technology String,
   type String
)
row format delimited
fields terminated by ','
stored as textfile;

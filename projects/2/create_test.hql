#hive script
CREATE DATABASE IF NOT EXISTS thekirillisupov;

USE thekirillisupov;

CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS  thekirillisupov.hw2_test
(  id INT,
   label INT,
   if1 INT,
   if1 INT,
   if2 INT,
   if3 INT,
   if4 INT,
   if5 INT,
   if6 INT,
   if7 INT,
   if8 INT,
   if9 INT,
   if10 INT,
   if11 INT,
   if12 INT,
   if13 INT,
   cat1 STRING,
   cat2 STRING,
   cat3 STRING,
   cat4 STRING,
   cat5 STRING,
   cat6 STRING,
   cat7 STRING,
   cat8 STRING,
   cat9 STRING,
   cat10 STRING,
   cat11 STRING,
   cat12 STRING,
   cat13 STRING,
   cat14 STRING,
   cat15 STRING,
   cat16 STRING,
   cat17 STRING,
   cat18 STRING,
   cat19 STRING,
   cat20 STRING,
   cat21 STRING,
   cat22 STRING,
   cat23 STRING,
   cat24 STRING,
   cat25 STRING,
   cat26 STRING,
   day_number STRING 
)
row format delimited
fields terminated by ','
stored as textfile;
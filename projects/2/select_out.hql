INSERT OVERWRITE DIRECTORY 'thekirillisupov_hiveout' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
select * from hw2_pred;

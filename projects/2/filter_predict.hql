ADD FILE predict.py;
SELECT TRANSFORM(*) 
USING 'python3 predict.py' 
FROM hw2_test WHERE if1 < 40 AND 20 < if1 LIMIT 10;

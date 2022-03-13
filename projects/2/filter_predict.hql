ADD FILE projects/2/predict.py;
INSERT INTO TABLE hw2_pred
SELECT TRANSFORM(*) 
USING 'python3 projects/2/predict.py'
AS (id INT, pred FLOAT) 
FROM hw2_test WHERE if1 < 40 AND 20 < if1;

ADD FILE projects/2/predict.py;
INSERT INTO TABLE hw2_pred
SELECT TRANSFORM(id,if1,if2,if3,if4,if5,if6,if7,if8,if9,if10,if11,if12,if13,cat1,cat2,cat3,cat4,cat5,cat6,cat7,cat8,cat9,cat10,cat11,cat12,cat13,cat14,cat15,cat16,cat17,cat18,cat19,cat20,cat21,cat22,cat23,cat24,cat25,cat26,day_number)*) 
USING 'python3 projects/2/predict.py'
AS (id INT, pred FLOAT) 
FROM hw2_test WHERE if1 < 40 AND 20 < if1;

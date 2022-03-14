ADD FILE projects/2/predict.py;
ADD FILE projects/2/model.py;
ADD FILE 2.joblib;
INSERT OVERWRITE TABLE hw2_pred
SELECT TRANSFORM(id,nvl(if1,0),nvl(if2,0),nvl(if3,0),nvl(if4,0),nvl(if5,0),nvl(if6,0),nvl(if7,0),nvl(if8,0),nvl(if9,0),nvl(if10,0),nvl(if11,0),nvl(if12,0),nvl(if13,0),cat1,cat2,cat3,cat4,cat5,cat6,cat7,cat8,cat9,cat10,cat11,cat12,cat13,cat14,cat15,cat16,cat17,cat18,cat19,cat20,cat21,cat22,cat23,cat24,cat25,cat26,day_number) 
USING 'predict.py'
AS id ,pred  
FROM hw2_test WHERE if1 < 40 AND 20 < if1;

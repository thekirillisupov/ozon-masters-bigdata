projects/1/train.sh 1 /home/users/datasets/criteo/criteo_train1 

arr_num = [i for i in range(14)]
arr_cat = [20,23,28,31,34,36,37]

def select(X):
  ans=[]
  s={"cf"+str(i) for i in range(1,27)}
  s.add("day_number")
  for x in X:
    if x in s:
      if len(X[x].unique()) < 100:
        ans.append(x)
  return ans
  

hdfs dfs -rm -r -f -skipTrash predicted.csv
projects/1/predict.sh projects/1/predict.py,projects/1/model.py,1.joblib /datasets/criteo/criteo_valid_large_features predicted.csv predict.py
yarn logs -applicationId application_1646131515886_0005 -out logs

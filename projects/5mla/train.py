#!/opt/conda/envs/dsenv/bin/python
import mlflow
import os, sys
#import logging

import pandas as pd
from sklearn.model_selection import train_test_split
#from joblib import dump

#
# Import model definition
#
from model import model, fields


#
# Logging initialization
#
#logging.basicConfig(level=logging.DEBUG)
#logging.info("CURRENT_DIR {}".format(os.getcwd()))
#logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
#logging.info("ARGS {}".format(sys.argv[1:]))

#
# Read script arguments
#
try:
  #proj_id = sys.argv[1] 
  train_path = sys.argv[1]
  param = sys.argv[2]
#except:
  #logging.critical("Need to pass both project_id and train dataset path")
  #sys.exit(1)


#logging.info(f"TRAIN_ID {proj_id}")
#logging.info(f"TRAIN_PATH {train_path}")

#
# Read dataset
#

read_table_opts = dict(sep="\t", names=fields, index_col=False)
df = pd.read_table(train_path, **read_table_opts)
#choose short cat features
arr_num = [i for i in range(15)]
arr_cat = [20,23,28,31,34,36,37]
arr_fin=arr_cat+arr_num
#df = df.iloc[:,sorted(arr_fin)]
#print(df.columns)
#split train/test
X_train, X_test, y_train, y_test = train_test_split(
    df.iloc[:,2:], df.iloc[:,1], test_size=0.33, random_state=41
)

#
# Train the model
#



with mlflow.start_run():
    model.fit(X_train, y_train)
    model_score = model.score(X_test, y_test)
    mlflow.log_metric("ROC AUC", model_score)
    mlflow.sklearn.log_model(model, "model")

#logging.info(f"model score: {model_score:.3f}")

# save the model
#dump(model, "{}.joblib".format(proj_id))



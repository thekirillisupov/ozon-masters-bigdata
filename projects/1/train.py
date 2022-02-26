#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging

import pandas as pd
from sklearn.model_selection import train_test_split
from joblib import dump

#
# Import model definition
#
from model import modelfull, fields, modelpreprocess 


#
# Logging initialization
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#
# Read script arguments
#
try:
  proj_id = sys.argv[1] 
  train_path = sys.argv[2]
except:
  logging.critical("Need to pass both project_id and train dataset path")
  sys.exit(1)


logging.info(f"TRAIN_ID {proj_id}")
logging.info(f"TRAIN_PATH {train_path}")

#
# Read dataset
# fileds = ["id","label"]+num_features 1 .. 13 + cat_features 1 .. 26 + "day_namber"

read_table_opts = dict(sep="\t", names=fields, index_col=False, chunksize=10000)
reader = pd.read_table(train_path, **read_table_opts)

df = pd.DataFrame()
for i, chunk in enumerate(reader): 
    if df.shape[0] > 1000000:
        break
    df = pd.concat([df, chunk.sample(frac=.05, replace=False, random_state=911)], axis=0)  


#split train/test
X_train, X_test, y_train, y_test = train_test_split(
    df.iloc[:,2:], df.iloc[:,1], test_size=0.33, random_state=42
)

#
# Train the model
#
modelfull.fit(X_train, y_train)
modelpreprocess.fit(X_test, y_test)
model_score = modelfull.score(X_test, y_test)

logging.info(f"model score: {model_score:.3f}")

# save the model
dump(modelfull, "{}.joblib".format(proj_id))



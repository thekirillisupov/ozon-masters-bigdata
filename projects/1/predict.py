#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fieldswithoutlabel, fields

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("1.joblib")


#read and infere
read_opts=dict(
        sep='\t', names=fieldswithoutlabel, index_col=False, header=None,
        iterator=True, chunksize=100
)

for df in pd.read_csv(sys.stdin, **read_opts):
    arr_num = [i for i in range(15)]
    arr_cat = [20, 23, 28, 31, 34, 36, 37]
    arr_fin= arr_cat+arr_num
    #df.iloc[:.sorted(arr_fin)]
    pred = model.predict_proba(df)[:, 1]
    out = zip(df.id, pred)
    print("\n".join(["{0}\t{1}".format(*i) for i in out]))


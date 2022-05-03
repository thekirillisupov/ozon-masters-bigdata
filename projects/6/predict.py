from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT
import joblib
import pyspark.sql.functions as F
import pandas as pd
from pyspark.ml.linalg import DenseVector

path_test = 'Eelect_test_out'
path_model = '6.joblib'
path_predict = 'Eelect_hw6_prediction'

schema = StructType(fields=[
    StructField("features", ArrayType(FloatType(),False))
])

df_test = spark.read.json('Eelect_test_out', schema=schema).cache()


est = joblib.load(path_model)
est_broadcast = spark.sparkContext.broadcast(est)


@F.pandas_udf(FloatType())
def predict(series):
    predictions = est_broadcast.value.predict(series.tolist()) # Don't forget to use tolist() method
    return pd.Series(predictions)

df_test = df_test.withColumn("prediction", predict("features")).select('prediction')
df_test.repartition(1).write.format("csv").mode("overwrite").save(path_predict)

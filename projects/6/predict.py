
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.functions import vector_to_array
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT
import joblib
import pyspark.sql.functions as F
import pandas as pd
from pyspark.ml.linalg import DenseVector
import sys

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

path_test = 'thekirillisupov_test_out'
path_model = sys.argv[1]
path_predict = 'thekirillisupov_hw6_prediction'

schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("features", ArrayType(FloatType(),False))
])

df_test = spark.read.json(path_test, schema=schema).cache()


est = joblib.load(path_model)
est_broadcast = spark.sparkContext.broadcast(est)


@F.pandas_udf(FloatType())
def predict(series):
    predictions = est_broadcast.value.predict(series.tolist())
    return pd.Series(predictions)

df_test = df_test.select("id", "features").withColumn("prediction", predict("features"))
df_test.select("id", "prediction").repartition(1).write.mode("overwrite").csv(path_predict)

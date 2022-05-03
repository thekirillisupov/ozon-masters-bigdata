import os
import sys

SPARK_HOME = "/usr/hdp/current/spark2-client"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.9.3-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))

from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark SQL").getOrCreate()

from pyspark.sql.types import *

schema_train = StructType([
    StructField("label", FloatType()),
    StructField("reviewText", StringType())
])


schema_test = StructType([
    StructField("reviewText", StringType())
])


train_path = sys.argv[1]
test_path = sys.argv[2]

train_df = spark.read.json(train_path, schema=schema_train).cache()
train_df = train_df.withColumn('reviewText', f.regexp_replace('reviewText', '[^A-Za-z0-9\s]+', ''))


test_df = spark.read.json(test_path, schema=schema_test).cache()
test_df = test_df.withColumn('reviewText', f.regexp_replace('reviewText', '[^A-Za-z0-9\s]+', ''))

train_df.write.json("thekirillisupov_train_out")
test_df.write.json("thekirllisupov_test_out")

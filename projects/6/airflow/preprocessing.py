import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import *
from pyspark.sql.functions import lower, col
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.functions import vector_to_array

from pyspark import SparkConf
from pyspark.sql import SparkSession

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

tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)
hasher = HashingTF(numFeatures=333, binary=True, inputCol=swr.getOutputCol(), outputCol="features")

train_df = tokenizer.transform(train_df)
train_df = swr.transform(train_df)
train_df = hasher.transform(train_df)
train_df = train_df.select(['label', 'features']).cache()
train_df.withColumn("features", vector_to_array("features")).show()
train_df = train_df.withColumn("features", vector_to_array("features"))

test_df = tokenizer.transform(test_df)
test_df = swr.transform(test_df)
test_df = hasher.transform(test_df)
test_df = test_df.select(['label', 'features']).cache()
test_df.withColumn("features", vector_to_array("features")).show()
test_df = test_df.withColumn("features", vector_to_array("features"))

train_df.write.json("thekirillisupov_train_out")
test_df.write.json("thekirllisupov_test_out")

from pyspark.ml.feature import *
from sklearn.metrics import classification_report, precision_score
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import Transformer
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline

schema = StructType([
    StructField("overall", FloatType()),
    #StructField("id", IntegerType()),
    #StructField("vote", StringType()),
    #StructField("verified", BooleanType()),
    #StructField("reviewTime", StringType()),
    #StructField("reviewerID", StringType()),
    #StructField("asin", StringType()),
    #StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    #StructField("summary", StringType()),
    #StructField("unixReviewTime", IntegerType())
])

path = sys.argv[1]
model_path = sys.argv[2]

df = spark.read.json(path, schema=schema).cache()

pipeline_model = pipeline.fit(train)
pipeline_model.write().overwrite().save(model_path)

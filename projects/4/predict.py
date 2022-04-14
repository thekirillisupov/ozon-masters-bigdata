import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import *
from sklearn.metrics import classification_report, precision_score
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import Transformer

model_path = sys.argv[1]
test_path = sys.argv[2]
predict_path = sys.argv[3]

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel

model = PipelineModel.load(model_path)

schema = StructType([
    #StructField("overall", FloatType()),
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

df_test = spark.read.json(test_path, schema=schema).cache()

predictions = pipeline_model.transform(df_test)

predictions.write.csv(predict_path)

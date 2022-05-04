from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lower, col
from pyspark.ml.feature import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.functions import vector_to_array

from pyspark import SparkConf
from pyspark.sql import SparkSession

path_out_train = 'thekirillisupov_train_out'

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema = StructType(fields=[
    StructField("label", IntegerType()),
    StructField("reviewText", StringType())
])
train_df = spark.read.json("/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json", schema=schema).cache()


tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)
hasher = HashingTF(numFeatures=300, binary=True, inputCol=swr.getOutputCol(), outputCol="features")


train_df = tokenizer.transform(train_df)
train_df = swr.transform(train_df)
train_df = hasher.transform(train_df)
train_df = train_df.select(['label', 'features']).cache()
train_df = train_df.withColumn("features", vector_to_array("features"))

train_df.repartition(1).write.format("json").mode("overwrite").save(path_out_train)



############TEST##########
path_out_test = 'thekirillisupov_test_out'


schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("reviewText", StringType())
])
train_df = spark.read.json("/datasets/amazon/all_reviews_5_core_test_extra_small_features.json", schema=schema).cache()

tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)
hasher = HashingTF(numFeatures=300, binary=True, inputCol=swr.getOutputCol(), outputCol="features")


train_df = tokenizer.transform(train_df)
train_df = swr.transform(train_df)
train_df = hasher.transform(train_df)
train_df = train_df.select(['id', 'features']).cache()
train_df = train_df.withColumn("features", vector_to_array("features"))

train_df.repartition(1).write.format("json").mode("overwrite").save(path_out_test)


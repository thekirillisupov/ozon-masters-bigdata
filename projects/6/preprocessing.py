
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

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema_train = StructType([
    StructField("label", FloatType()),
    StructField("reviewText", StringType())
])


schema_test = StructType([
    StructField("reviewText", StringType())
])


train_path = "/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json"
test_path = "/datasets/amazon/all_reviews_5_core_test_extra_small_features.json"

train_df = spark.read.json(train_path, schema=schema_train).cache()
#train_df = train_df.withColumn('reviewText', f.regexp_replace('reviewText', '[^A-Za-z0-9\s]+', ''))

test_df = spark.read.json(test_path, schema=schema_test).cache()
#test_df = test_df.withColumn('reviewText', f.regexp_replace('reviewText', '[^A-Za-z0-9\s]+', ''))

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
test_df = test_df.select(['features']).cache()
test_df.withColumn("features", vector_to_array("features")).show()
test_df = test_df.withColumn("features", vector_to_array("features"))

train_df.repartition(1).write.format("json").mode("overwrite").save(thekirllisupov_train_out)
test_df.repartition(1).write.format("json").mode("overwrite").save(thekirllisupov_test_out)

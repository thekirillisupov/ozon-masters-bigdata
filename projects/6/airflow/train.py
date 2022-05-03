from pyspark.ml.feature import *
from sklearn.metrics import classification_report, precision_score
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import Transformer


tokenizerBig = Tokenizer(inputCol="reviewText", outputCol="wordsBig")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizerBig.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)
count_vectorizerBig = CountVectorizer(inputCol=swr.getOutputCol(), outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="overall", maxIter=10, regParam=0)

pipeline = Pipeline(stages=[
    tokenizerBig,
    swr,
    count_vectorizerBig,
    lr
])

model_path = sys.argv[1]
train_path = sys.argv[2]

df_train = spark.read.json(train_path)
pipeline_model = pipeline_model.fit(df_train)
pipeline_model.write().overwrite().save(model_path)

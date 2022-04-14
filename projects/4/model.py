from pyspark.ml.feature import *
from sklearn.metrics import classification_report, precision_score
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import Transformer


class Remove_spec_char(Transformer):
    def __init__(self, inputCol, outputCol='reviewText_clear'):
        self.inputCol = inputCol #the name of your columns
        self.outputCol = outputCol #the name of your output column

    def this():
        #define an unique ID
        this(Identifiable.randomUID("Removespecchar"))

    def copy(extra):
        defaultCopy(extra)

    #def getOutputCol(self):
    #    return self.outputCol

    def check_input_type(self, schema):
        field = schema[self.inputCol]
        #assert that field is a datetype
        if (field.dataType != StringType()):
            raise Exception('Remove_spec_char input type %s did not match input type StringType' % field.StringType)

    def _transform(self, df):
        self.check_input_type(df.schema)
        return df.withColumn(self.outputCol, f.regexp_replace(self.inputCol, '[^A-Za-z0-9\s]+', ''))

#remove_spec_char = Remove_spec_char(inputCol="reviewText")
tokenizerBig = Tokenizer(inputCol="reviewText", outputCol="wordsBig")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizerBig.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)
count_vectorizerBig = CountVectorizer(inputCol=swr.getOutputCol(), outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="overall", maxIter=10, regParam=0)

pipeline = Pipeline(stages=[
    #remove_spec_char,
    tokenizerBig,
    #tokenizerSmall,
    swr,
    count_vectorizerBig,
    #count_vectorizerSmall,
    #assembler,
    lr
])

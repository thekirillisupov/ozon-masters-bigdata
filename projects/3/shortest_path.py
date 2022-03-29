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

program_name = sys.argv[0]
start = int(sys.argv[1])
end = int(sys.argv[2])
path_csv = sys.argv[3]
out_csv = sys.argv[4]

from pyspark.sql.types import *
schema = StructType(fields=[
    StructField("user_id", IntegerType()),
    StructField("follower_id", IntegerType())
])
df = spark.read\
          .schema(schema)\
          .format("csv")\
          .option("sep", "\t")\
          .load(path_csv)


from pyspark.sql.functions import map_from_arrays, create_map, collect_list
from pyspark.sql.functions import lit

df_sort = df.orderBy('follower_id')
new_list = df_sort.groupby("follower_id").agg(collect_list("user_id").alias("users_id"))
new_list = new_list.withColumn('new_column', lit(1))
df_for_maps = new_list.groupBy('new_column').agg(collect_list("follower_id").alias("new_users_id"),\
                                   collect_list("users_id").alias("new_follower_id"))
df_result = df_for_maps.select(map_from_arrays(df_for_maps.new_users_id, df_for_maps.new_follower_id).alias('result')).cache()


class Node:
    def __init__(self, value, pred=None, dist=None):
        self.value = value
        self.pred = []
        self.distance = dist
        self.path = []


class Queue:
    def __init__(self):
        self.queue = []
        self.start = 0
        self.end = 0

    def insert(self, x):
        self.end += 1
        self.queue.append(x)

    def pop(self):
        if self.end - self.start <= 0:
            return None
        else:
            self.start += 1
            return self.queue[self.start - 1]

    def is_empty(self):
        return self.start == self.end


def get_childs(df_result, value):
    return df_result.select(df_result.result.getItem(value)).first()[0]


dict_complete = {}
queue = Queue()

node_start = Node(start, dist=0)
dict_complete[start] = node_start
queue.insert(node_start)

max_dist = 0

while not queue.is_empty():
    current_node = queue.pop()
    current_dist = current_node.distance

    if max_dist:
        if current_dist >= max_dist:
            break

    # get childs pyspark
    childs = get_childs(df_result, current_node.value)
    if childs is None:
        continue
    for child in childs:

        if child in dict_complete:
            child_node = dict_complete[child]
            if child_node.distance == current_dist + 1:
                child_node.pred.append(current_node)

        else:
            node_child = Node(child, dist=current_dist + 1)
            node_child.pred.append(current_node)
            dict_complete[child] = node_child
            queue.insert(node_child)
            if child == end:
                max_dist = current_dist + 1
                results = node_child

def create_ways(result):
    dict_complete = {}
    start_path = [[]]
    current_node = result
    current_node.path = start_path
    queue = Queue()
    queue.insert(current_node)
    dict_complete[current_node.value] = 1
    while True:
        current_node = queue.pop()
        current_node.path = [x + [current_node.value] for x in current_node.path]

        if current_node.pred == []:
            return current_node.path

        for pred_node in current_node.pred:

            pred_node.path += current_node.path

            if pred_node.value not in dict_complete:
                dict_complete[pred_node.value] = 1
                queue.insert(pred_node)


ways = create_ways(results)
ways = [way[::-1] for way in ways]
data = ways
spark_1 = SparkSession.builder.config(conf=conf).appName("well").getOrCreate()
df = spark_1.createDataFrame(data)

df.write.csv(out_csv, sep=',')

from pyspark import SparkContext, SparkConf
import sys

 
#conf = SparkConf()
#sc = SparkContext(appName="Pagerank", conf=conf)
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark SQL").getOrCreate()

df = spark.read.csv(str(sys.argv[3]),sep="\t").cache()

node1 = int(sys.argv[1])
node2 = int(sys.argv[2])

n1 = df.filter(df._c0 == 34)

n2 = df.filter(df._c1 == 12)

n1.createOrReplaceTempView("n1_table")

n2.createOrReplaceTempView("n2_table")

def shortest_path(node1, node2):
    path_list = [[node1]]
    path_index = 0
    # To keep track of previously visited nodes
    previous_nodes = {node1}
    if node1 == node2:
        return path_list[0]
        
    while path_index < len(path_list):
        current_path = path_list[path_index]
        last_node = current_path[-1]
        next_nodes = df.filter(df._c0 == last_node).select(df._c1).rdd.map(lambda x: (int(x[0]))).collect() # here add df.filter(df._c1 == last_node)
        # Search goal node
        if node2 in next_nodes:
            current_path.append(node2)
            return current_path[::-1]
        # Add new paths
        for next_node in next_nodes[:4]: # optimise param
            if not next_node in previous_nodes:
                new_path = current_path[:]
                new_path.append(next_node)
                path_list.append(new_path)
                # To avoid backtracking
                previous_nodes.add(next_node)
        # Continue to next path in list
        path_index += 1
    # No path is found
    return []

res=shortest_path(node2, node1)

resStr=''
for i in range(len(res)):
    resStr += str(res[i])+','
resStr[:-1]

query = """
SELECT n1_table._c0, n1_table._c1, n2_table._c1
FROM
n1_table JOIN n2_table
ON n1_table._c1 = n2_table._c0
"""

res = spark.sql(query)

#resRow

resRow = res.collect()

data = []
for i in range(len(resRow)):
    resRowstr = ''
    for j in range(3):
        resRowstr += resRow[i][2-j] + ','
    data.append(resRowstr[:-1])

#data

from pyspark.sql import Row

resdf = spark.createDataFrame([
    Row(data[0]),
    Row(data[1]),
    Row(data[2]),
    Row(data[3]),
    Row(data[4])
])
#resdf

#file1 = open(str(sys.argv[4])+".csv", "w")  # write mode
#file1.write(resStr[:-1])
#file1.close()

#resdf = spark.createDataFrame([
#    Row(resStr[:-1])
#])

resdf.write.csv(str(sys.argv[4]))

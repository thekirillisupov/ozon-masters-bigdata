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

#file1 = open(str(sys.argv[4])+".csv", "w")  # write mode
#file1.write(resStr[:-1])
#file1.close()

from pyspark.sql import Row

resdf = spark.createDataFrame([
    Row(resStr[:-1])
])

resdf.write.csv(str(sys.argv[4]))

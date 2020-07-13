from pyspark import SparkContext
from pyspark.sql import SQLContext
from graphframes import *
from itertools import combinations
import sys
import os


def write_output(communities):

    with open(outputPath, 'w') as f:
        for community in communities:
            line = str(community).strip("[]") + "\n"
            f.write(line)


if __name__ == "__main__":

    filterThreshold = 7#sys.argv[1]
    inputFilepath = 'ub_sample_data.csv'#sys.argv[2]
    outputPath = 'task1.res'#sys.argv[3]
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(master = "local[*]", appName = "hw4task1")
    sc.setLogLevel("ERROR")

    data = sc.textFile(inputFilepath).map(lambda line: line.split(","))
    header = data.first()
    data = data.filter(lambda x: x != header)
    ub = sc.broadcast(data.groupByKey().mapValues(set).filter(lambda x: len(x[1]) >= 7).collectAsMap())

    users = ub.value.keys()
    edges = sc.parallelize(combinations(users, 2)).filter(lambda x: len(ub.value[x[0]] & ub.value[x[1]]) >= filterThreshold).persist()
    allEdges = edges.union(edges.map(lambda x: (x[1], x[0])))

    vertices = edges.flatMap(lambda x: x).distinct().map(lambda x: tuple(x.split()))

    sqlC = SQLContext(sc)
    v = sqlC.createDataFrame(vertices, ["id"])
    e = sqlC.createDataFrame(allEdges, ["src", "dst"])
    g = GraphFrame(v, e)

    result = g.labelPropagation(maxIter=5)
    communities = sorted(result.rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(sorted).values().collect(), key = lambda x: (len(x), x))
    write_output(communities)


from pyspark import SparkContext
from math import sqrt
import json
import sys
from time import time

def printOutput(similarity):

    with open(outputFile, 'w') as f:
        for x in similarity:
            line = {"user_id": x[0][0], "business_id": x[0][1], "sim": x[1]}
            f.write(json.dumps(line) + "\n")


if __name__ == "__main__":
    start = time()
    testFile = 'test_review.json'#sys.argv[1]
    modelFile = 'task2.model'#sys.argv[2]
    outputFile = 'task2.predict'#sys.argv[3]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(master = 'local[*]', appName = "hw3task2predict")
    sc.setLogLevel("ERROR")

    pairs = sc.textFile(testFile).map(json.loads).map(lambda x: (x['user_id'], x['business_id']))
    users = sc.broadcast(pairs.keys().distinct().collect())
    business = sc.broadcast(pairs.values().distinct().collect())
    model = sc.textFile(modelFile).map(json.loads).map(lambda x: (x['id'], x['profile']))
    userProfile = model.filter(lambda x: x[0] in users.value)
    businessProfile = model.filter(lambda x: x[0] in business.value)
    similarity = pairs.join(userProfile).map(lambda x: (x[1][0], (x[0], x[1][1]))).join(businessProfile). \
        map(lambda x: ((x[1][0][0], x[0]), len(set(x[1][0][1]).intersection(x[1][1])) / (sqrt(len(x[1][0][1])) * sqrt(len(x[1][1]))))).\
        filter(lambda x: x[1] >= 0.01).collect()
    printOutput(similarity)
    print(time() - start)

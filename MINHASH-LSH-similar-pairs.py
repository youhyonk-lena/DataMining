from pyspark import SparkContext
from itertools import combinations
import json
import sys
from time import time

def minHash(ids):

    signature = []
    p = 51971

    for i in range(1, numSig + 1):

        sig = min([int((((i * (hash(x) % ((sys.maxsize + 1) * 2)))**2 + i) % p) % numUsers) for x in ids])
        signature.append(sig)

    return signature


def lsh(x):

    return int((((1097 * sum([x[s] * s for s in range(len(x))])) + 67) % 68581) % int(numUsers * .6))


def getSim(x):

    sim = len(set(businessUser.value[x[0]]) & set(businessUser.value[x[1]])) / len(set(businessUser.value[x[0]]) | set(businessUser.value[x[1]]))
    if sim >= 0.05:
        return {"b1": x[0], "b2": x[1], "sim": sim}
    else:
        return 0


def printOutput(similar):

    with open(outputFile, 'w') as f:

        for x in similar:
            f.write(json.dumps(x))
            f.write("\n")


if __name__ == "__main__":

    start = time()
    inputFile = 'train_review.json'#sys.argv[1]
    outputFile = 'task1.res'#sys.argv[2]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(appName="hw3task1", master="local[*]")
    sc.setLogLevel("ERROR")

    start = time()
    data = sc.textFile(inputFile).map(json.loads).map(lambda x: (x["business_id"], x["user_id"])).distinct().persist()
    users = sc.broadcast(data.values().distinct().zipWithUniqueId().collectAsMap())
    numUsers = len(users.value)
    data.unpersist()

    numSig = 52
    businessUser = data.mapValues(lambda x: [users.value[x]]).reduceByKey(lambda x, y: x + y)
    sigMat = businessUser.mapValues(minHash).persist()

    r = 2
    i = 0
    candidates = sc.parallelize([])
    while i < numSig:

        cands = sigMat.map(lambda x: (lsh(x[1][i: i + r]), x[0])).groupByKey().flatMap(lambda x: combinations(sorted(x[1]), 2)).persist()
        candidates = candidates.union(cands).persist()
        i += r

    candidates = candidates.distinct().persist()
    sigMat.unpersist()

    businessUser = sc.broadcast(businessUser.collectAsMap())
    similar = candidates.map(getSim).filter(lambda x: x != 0).collect()
    printOutput(similar)

    print(time() - start)

from pyspark import SparkContext
from math import sqrt
from itertools import combinations
import json
import sys
from time import time


def printBusiness(pearson):

    with open(modelFile, 'w')as f:

        for x in pearson:
            line = {"b1": x[0][0], "b2": x[0][1], "sim": x[1]}
            f.write(json.dumps(line))
            f.write("\n")

def printUser(pearson):

    with open(modelFile, 'w')as f:

        for x in pearson:
            line = {"u1": x[0][0], "u2": x[0][1], "sim": x[1]}
            f.write(json.dumps(line))
            f.write("\n")


def getItemPearson(business):

    b1 = business[0]
    b2 = business[1]

    coRated = set(businessUser.value[b1]) & set(businessUser.value[b2])
    b1avg = sum(ubStar.value[(user, b1)] for user in coRated) / len(coRated)
    b2avg = sum(ubStar.value[(user, b2)] for user in coRated) / len(coRated)

    numerator = 0
    denomLeft = 0
    denomRight = 0
    for user in coRated:
        b1Rating = ubStar.value[(user, b1)]
        b2Rating = ubStar.value[(user, b2)]
        numerator += ((b1Rating - b1avg) * (b2Rating - b2avg))
        denomLeft += (b1Rating - b1avg)**2
        denomRight += (b2Rating - b2avg)**2

    if denomLeft * denomRight == 0:
        pearson = 0
    else:
        pearson = numerator / (sqrt(denomLeft) * sqrt(denomRight))

    return (business, pearson)


def getUserPearson(users):

    u1 = users[0]
    u2 = users[1]

    coRated = set(userBusiness.value[u1]) & set(userBusiness.value[u2])
    u1avg = sum(buStar.value[(business, u1)] for business in coRated) / len(coRated)
    u2avg = sum(buStar.value[(business, u2)] for business in coRated) / len(coRated)

    numerator = 0
    denomLeft = 0
    denomRight = 0
    for business in coRated:
        u1Rating = buStar.value[(business, u1)]
        u2Rating = buStar.value[(business, u2)]
        numerator += ((u1Rating - u1avg) * (u2Rating - u2avg))
        denomLeft += (u1Rating - u1avg)**2
        denomRight += (u2Rating - u2avg)**2

    if denomLeft * denomRight == 0:
        pearson = 0
    else:
        pearson = numerator / (sqrt(denomLeft) * sqrt(denomRight))

    return (users, pearson)


def minHash(ids):

    signature = []
    p = 51971

    for i in range(1, numSig + 1):
        sig = min([int(((i * (hash(x) % ((sys.maxsize + 1) * 2))**2 + i) % p) % numUsers) for x in ids])
        signature.append(sig)

    return signature


def lsh(x):

    return int((((1097 * sum([x[s] * s for s in range(len(x))])) + 67) % 68581) % int(numUsers * .5))



if __name__ == "__main__":
    start = time()
    trainFile = 'train_review.json'#sys.argv[1]
    modelFile = 'task3user.model'#sys.argv[2]
    cf_type = "user_based"#sys.argv[3]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(appName="hw3task1", master="local[*]")
    sc.setLogLevel("ERROR")
    review = sc.textFile(trainFile).map(json.loads).map(lambda x: (x["user_id"], x["business_id"], x["stars"]))

    if cf_type == "item_based":

        user = sc.broadcast(review.keys().zipWithUniqueId().collectAsMap())
        ubStar = sc.broadcast(review.map(lambda x: ((user.value[x[0]], x[1]), x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        bAvg = sc.broadcast(review.map(lambda x: (x[1], x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        businessUser = sc.broadcast(review.map(lambda x: (x[1], user.value[x[0]])).groupByKey().mapValues(lambda x: list(set(x))).collectAsMap())
        business = review.map(lambda x: x[1]).distinct()
        pearson = business.cartesian(business).filter(lambda x: x[0] != x[1]).map(lambda x: tuple(sorted(x))).\
            filter(lambda x: len(set(businessUser.value[x[0]]) & set(businessUser.value[x[1]])) >= 3).map(getItemPearson). \
            filter(lambda x: x[1] > 0).distinct().collect()
        printBusiness(pearson)
        print(time() - start)

    elif cf_type == "user_based":

        user = set(review.keys().collect())
        numUsers = len(user)
        business = sc.broadcast(review.map(lambda x: x[1]).zipWithUniqueId().collectAsMap())
        buStar = sc.broadcast(review.map(lambda x: ((business.value[x[1]], x[0]), x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        uAvg = sc.broadcast(review.map(lambda x: (x[0], x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        userBusiness = review.map(lambda x: (x[0], business.value[x[1]])).groupByKey().mapValues(lambda x: list(set(x)))
        sigMat = userBusiness.mapValues(minHash)

        numSig = 60
        r = 2
        i = 0
        candidates = sc.parallelize([])
        while i < numSig:

            cands = sigMat.map(lambda x: (lsh(x[1][i: i + r]), x[0])).groupByKey().flatMap(lambda x: combinations(sorted(x[1]), 2)).persist()
            candidates = candidates.union(cands).persist()
            i += r

        candidates = candidates.distinct().persist()
        userBusiness = sc.broadcast(userBusiness.collectAsMap())
        pearson = candidates.filter(lambda x: len(set(userBusiness.value[x[0]]) & set(userBusiness.value[x[1]])) >= 3 and len(set(userBusiness.value[x[0]]) & set(userBusiness.value[x[1]])) / len(set(userBusiness.value[x[0]]) | set(userBusiness.value[x[1]])) >= 0.01). \
            map(getUserPearson).filter(lambda x: x[1] > 0).distinct().collect()
        printUser(pearson)
        print(time() - start)

    else:

        print("wrong input")
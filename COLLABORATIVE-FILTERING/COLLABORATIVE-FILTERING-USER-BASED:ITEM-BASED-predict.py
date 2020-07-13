from pyspark import SparkContext
from operator import add
import json
import sys
from time import time


def predictItem(p):

    user = p[0]
    item = p[1]

    star = avg

    if item in bAvg.value.keys():

        if item in bSim.value.keys():

            closest = sorted(filter(lambda x: (user, x[0]) in ubStar.value.keys(), bSim.value[item]), key = lambda x: -x[1])[:n]

            if len(closest):
                numer = sum([ubStar.value[(user, x[0])] * x[1] for x in closest])
                denom = sum([x[1] for x in closest])

                if len(closest) == n:
                    star = numer / denom

                elif len(closest) < n:
                    numer += avg * (n - len(closest))
                    denom += avg * (n - len(closest))
                    star = numer / denom
            else:
                pass

        else:

            star = bAvg.value[item]

    else:
        pass

    return {"user_id": user, "business_id": item, "stars": star}


def predictUser(p):

    user = p[0]
    item = p[1]

    star = avg

    if user in uAvg.value.keys():

        if user in uSim.value.keys():
            closest = sorted(filter(lambda x: (x[0], item) in ubStar.value.keys(), uSim.value[user]), key = lambda x: -x[1])[:n]

            if len(closest):
                otherAvg = {}
                for x in closest:
                    u = x[0]
                    coRated = list(userBusiness.value[user].intersection(userBusiness.value[u]))
                    otherAvg[u] = sum(ubStar.value[(u, x)] for x in coRated) / len(coRated)
                numer = sum([(ubStar.value[(x[0], item)] - otherAvg[x[0]]) * x[1] for x in closest])
                denom = sum([x[1] for x in closest])
                star = uAvg.value[user] + (numer / denom)
            else:
                pass
        else:
             star = uAvg.value[user]

    else:
        pass

    return {"user_id": user, "business_id": item, "stars": star}


def printOutput(prediction):

    with open (outputFile, 'w') as f:
        for p in prediction:
            f.write(json.dumps(p))
            f.write("\n")



if __name__ == "__main__":
    start = time()
    trainFile = 'train_review.json'#sys.argv[1]
    testFile = 'test_review.json'#sys.argv[2]
    modelFile = 'task3user.model'#sys.argv[3]
    outputFile = 'task3user.predict'#sys.argv[4]
    cf_type = "user_based"#sys.argv[5]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(appName="hw3task1", master="local[*]")
    sc.setLogLevel("ERROR")
    review = sc.textFile(trainFile).map(json.loads).map(lambda x: (x["user_id"], x["business_id"], x["stars"]))
    pairs = sc.textFile(testFile).map(json.loads).map(lambda x: (x["user_id"], x["business_id"]))
    avg = review.map(lambda x: (1, x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).values().collect()[0]
    ubStar = sc.broadcast(review.map(lambda x: ((x[0], x[1]), x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
    n = 6

    if cf_type == "item_based":

        model = sc.textFile(modelFile).map(json.loads).map(lambda x:((x["b1"], x["b2"]), x["sim"]))
        bAvg = sc.broadcast(review.map(lambda x: (x[1], x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        bSim1 = model.map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(add)
        bSim2 = model.map(lambda x: (x[0][1], [(x[0][0], x[1])])).reduceByKey(add)
        bSim = sc.broadcast(bSim1.union(bSim2).reduceByKey(add).mapValues(set).collectAsMap())
        prediction = pairs.map(predictItem).collect()
        printOutput(prediction)
        print(time() - start)
    elif cf_type == "user_based":

        model = sc.textFile(modelFile).map(json.loads).map(lambda x:((x["u1"], x["u2"]), x["sim"]))
        uAvg = sc.broadcast(review.map(lambda x: (x[0], x[2])).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap())
        userBusiness = sc.broadcast(review.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set).collectAsMap())
        uSim1 = model.map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(add)
        uSim2 = model.map(lambda x: (x[0][1], [(x[0][0], x[1])])).reduceByKey(add)
        uSim = sc.broadcast(uSim1.union(uSim2).reduceByKey(add).mapValues(set).collectAsMap())
        prediction = pairs.map(predictUser).collect()
        printOutput(prediction)
        print(time() - start)
    else:

        print("wrong input")
from pyspark import SparkContext
from operator import add
from math import log2
import json
import string
import sys
from time import time


def clean(x):

    x = x.lower().translate(str.maketrans('', '', string.punctuation + '1234567890')).strip().split()
    text = [w for w in x if w not in stopwords.value]

    return text


def getProfile(x):

    s = sorted(x, key = lambda x: x[1], reverse = True)
    max = s[0][1]
    tfIdf = sorted([(words.value[tup[0]], (tup[1] / max) * idf.value[tup[0]]) for tup in s], key = lambda x: x[1], reverse = True)[:200]

    return [tup[0] for tup in tfIdf]


def printOutput(userProfile, businessProfile):

    with open(modelFile, 'w') as f:

        for x in userProfile:

            line = {"id": x[0], "profile": x[1]}
            f.write(json.dumps(line))
            f.write("\n")

        for x in businessProfile:

            line = {"id": x[0], "profile": x[1]}
            f.write(json.dumps(line))
            f.write("\n")



if __name__ == "__main__":

    start = time()
    inputFile = 'train_review.json'#sys.argv[1]
    modelFile = 'task2.model'#sys.argv[2]
    stopwordsDir = 'stopwords'#sys.argv[3]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(appName="hw3task2", master="local[*]")
    sc.setLogLevel("ERROR")

    stopwords = sc.broadcast(sc.textFile(stopwordsDir).collect())
    review = sc.textFile(inputFile).map(json.loads)

    numDocs = sc.broadcast(review.map(lambda x: x['business_id']).distinct().count())
    bText = review.map(lambda x: (x['business_id'], clean(x['text']))).reduceByKey(add).flatMapValues(lambda x: x)
    words = bText.map(lambda x: (x[1], 1)).reduceByKey(add)
    numWords = words.count()
    words = sc.broadcast(words.filter(lambda x: x[1] / numWords >= 0.000005).keys().zipWithUniqueId().collectAsMap())
    bText = bText.filter(lambda x: x[1] in words.value.keys())
    idf = sc.broadcast(bText.groupBy(lambda x: x[1]).mapValues(lambda x: log2(numDocs.value / len(set(x)))).collectAsMap())
    businessProfile = bText.map(lambda x: (x, 1)).reduceByKey(add).map(lambda x: (x[0][0], [(x[0][1], x[1])])).\
        reduceByKey(add).mapValues(getProfile)
    businessUser = review.map(lambda x: (x['business_id'], x['user_id'])).distinct()
    userProfile = businessUser.join(businessProfile).values().reduceByKey(add).mapValues(lambda x: list(set(x))).collect()
    businessProfile = businessProfile.collect()
    printOutput(userProfile, businessProfile)
    print(time() - start)






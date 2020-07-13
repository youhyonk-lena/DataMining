from pyspark import SparkContext
from math import log
from binascii import hexlify
import json
import csv
import sys


def setFilter(x):

    index = [((i * x + 3894) % 38069) % n for i in range(382, 382 + k)]
    return index


def checkFilter(x):

    index = [((i * x + 3894) % 38069) % n for i in range(382, 382 + k)]
    seen = False

    for ix in index:
        if bloomFilter[ix] == 0:
            seen = False
        else:
            seen = True

    return int(seen)


def writeOutput(result):

    with open(outputFile, 'w', newline='') as f:

        wr = csv.writer(f, delimiter = ' ')
        wr.writerow(result)


if __name__ == "__main__":

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(master = "local[*]", appName = "hw5task1")
    sc.setLogLevel("ERROR")

    firstJson = 'business_first.json'#sys.argv[1]
    secondJson = 'business_second.json'#sys.argv[2]
    outputFile = 'task1ans.csv'#sys.argv[3]

    first = sc.textFile(firstJson).map(json.loads).map(lambda x: x['city']).distinct()
    second = sc.textFile(secondJson).map(json.loads)
    k = 1
    m = second.count()
    n = round(k / log(2) * m)
    bloomFilter = [0] * n
    idx = first.flatMap(lambda x: setFilter(int(hexlify(x.encode('utf16')), 16))).distinct().collect()
    for i in idx:
        bloomFilter[i] = 1

    result = second.map(lambda x: checkFilter(int(hexlify(x['city'].encode('utf16')), 16))).collect()
    writeOutput(result)
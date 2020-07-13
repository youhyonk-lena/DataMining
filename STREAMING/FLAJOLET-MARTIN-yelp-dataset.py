import csv
import json
import random
from statistics import median
from binascii import hexlify
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def hashFunc(numbered, seed):

    random.seed(seed)
    a = random.randint(1361, 2221)
    random.seed(seed)
    b = random.randint(479, 727)

    hashed = max([getTrailingR((a * x + b) % BUCKETS) for x in numbered])

    return hashed


def getTrailingR(number):

    binary = bin(number)
    r = len(binary) - len(binary.rstrip('0'))

    return r


def flajoletMartin(time, rdd):

    groundTruth = rdd.distinct().count()
    numbered = rdd.map(lambda x: int(hexlify(x.encode('utf16')), 16)).collect()
    numGroups = 10
    hashPerGroup = int(NUMHASH / numGroups)
    averages = []
    for n in range(numGroups):
        group = []
        for h in range(hashPerGroup):
            seed = ((n + 1) * 100) + (h + 1)
            r = hashFunc(numbered, seed)
            group.append(2**r)

        avg = round(sum(group)/len(group))
        averages.append(avg)
    estimate = int(median(averages))

    if estimate <= 0.5 * groundTruth or estimate >= 1.5 * groundTruth:
        print("wrong result")
        
    with open(outputFile, 'a', newline='') as f:
        wr = csv.writer(f, delimiter = ',')
        line = [str(time), str(groundTruth), str(estimate)]
        wr.writerow(line)
        f.close()


WINDOW = 30
INTERVAL = 10
BUCKETS = 131
NUMHASH = 60


if __name__ == "__main__":

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(master = "local[*]", appName = "hw5task2")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)

    port = 9999#sys.argv[1]
    outputFile = 'task2ans.csv'#sys.argv[2]

    with open(outputFile, 'w', newline='') as f:
        wr = csv.writer(f, delimiter = ',')
        header = ['Time,Ground Truth,Estimation']
        wr.writerow(header)
        f.close()

    city = ssc.socketTextStream("localhost", port).window(WINDOW, INTERVAL).map(json.loads).map(lambda x: x['city'])
    city.foreachRDD(flajoletMartin)

    ssc.start()
    ssc.awaitTermination()

    ssc.stop()
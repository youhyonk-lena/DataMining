from pyspark import SparkContext
from itertools import combinations
from operator import add
from time import time
import sys

def format(line, length):

    output = ''

    if length == 1:
        for l in line:
            output += '(\'' + str(l) + '\')'
            if l == line[-1]:
                output += '\n'
            else:
                output += ','
    else:
        for l in line:
            output += str(l)
            if l == line[-1]:
                output += '\n'
            else:
                output += ','

    return output


def writeOutput(candidates, frequent):

    length = 1
    line = sorted(filter(lambda x: type(x) == str, candidates))

    with open(outputFilePath, 'w') as f:
        f.write("Candidates:\n")
        while line:
            output = format(line, length)
            f.write(output)
            length += 1
            line = sorted(filter(lambda x: type(x) != str and len(x) == length, candidates))
        f.write('\nFrequent Itemsets:\n')
        length = 1
        line = sorted(filter(lambda x: type(x) == str, frequent))
        while line:
            output = format(line, length)
            f.write(output)
            length += 1
            line = sorted(filter(lambda x: type(x) != str and len(x) == length, frequent))

    print("Duration: " + str(time()-start))


def hashFunction1(items):

    return (sum(items) % numBuckets, 1)


def hashFunction2(items):

    return ((sum(items) + 7) % numBuckets, 1)


def monotonicity(iterable, iteration, prevCand):

    survived = []
    x = sorted(set([i for tup in iterable for i in tup]))
    comb = list(combinations(x, iteration))

    for c in comb:
        allIn = True
        for subset in list(combinations(c, iteration - 1)):
            if subset in prevCand.value:
                pass
            else:
                allIn = False

        if allIn:
            survived.append(c)
        else:
            pass

    return survived


def getId(x):

    new = []
    for elem in x:
        new.append(businessId.value[elem])

    return new


def isCand(x, cand):

    support = []
    for c in cand:
        isC = True
        if type(c) == str:
            if c in x:
                pass
            else:
                isC = False
        else:
            for elem in c:
                if elem in x:
                    pass
                else:
                    isC = False
        if isC:
            support.append((c, 1))
        else:
            pass

    return support


def getName(tup):

    if type(tup) == int:
        return idBusiness.value[tup]
    else:
        return tuple([idBusiness.value[x] for x in tup])



def multiHash(chunk):

    iteration = 1
    baskets = sc.parallelize(chunk).zipWithUniqueId().map(lambda x: (x[1], getId(x[0]))).flatMapValues(lambda x: x).persist()
    adjustedThreshold = support * (len(chunk) / totalBaskets)
    candidates = []
    exists = True

    #First iteration
    single = sc.broadcast(baskets.map(lambda x: (x[1], 1)).reduceByKey(add).filter(lambda x: x[1] >= adjustedThreshold).keys().collect())
    candidates.append(single.value)
    iteration += 1
    combBasket = baskets.filter(lambda x: x[1] in single.value).groupByKey().mapValues(lambda x: list(combinations(x, iteration))).flatMapValues(lambda x: x).persist()

    while exists:

        #multihash pass 1
        bitmap1 = sc.broadcast(combBasket.map(lambda x: hashFunction1(x[1])).reduceByKey(add).filter(lambda x: x[1] >= adjustedThreshold).keys().collect())
        bitmap2 = sc.broadcast(combBasket.map(lambda x: hashFunction2(x[1])).reduceByKey(add).filter(lambda x: x[1] >= adjustedThreshold).keys().collect())

        #multihash pass 2
        cands = combBasket.map(lambda x: (x[1], hashFunction1(x[1]))).filter(lambda x: x[1][0] in bitmap1.value). \
            map(lambda x: (x[0], hashFunction2(x[0]))).filter(lambda x: x[1][0] in bitmap2.value). \
            map(lambda x: (tuple(x[0]), 1)).reduceByKey(add).filter(lambda x: x[1] >= adjustedThreshold).keys().collect()



        if cands:
            candidates.append(cands)
            prevCand = sc.broadcast(cands)
            iteration += 1
            combBasket = combBasket.filter(lambda x: x[1] in prevCand.value).groupByKey().mapValues(lambda x: monotonicity(x, iteration, prevCand)).flatMapValues(lambda x: x).persist()

        else:
            exists = False

    return candidates


def flatten(doubleList):

    flat = set()
    merge = doubleList[0] + doubleList[1]
    for lines in merge:
        for items in lines:
            flat.add(getName(items))

    return flat


if __name__ == "__main__":

    start = time()
    filterThreshold = 70#int(sys.argv[1])
    support = 50#int(sys.argv[2])
    inputFilePath = 'preprocess.csv'#sys.argv[3]
    outputFilePath = 'task2.output'#sys.argv[4]
    sc = SparkContext(appName = "hw2task2", master ='local[*]')
    sc.setLogLevel("ERROR")
    data = sc.textFile(inputFilePath).map(lambda x: x.split(",")).filter(lambda x: x != ['user_id', 'business_id']).persist()
    businessId = sc.broadcast(data.values().distinct().sortBy(lambda x: x).zipWithUniqueId().collectAsMap())
    idBusiness = sc.broadcast({y:x for x,y in businessId.value.items()})
    numBuckets = int(data.count() * .5)

    userBusiness = data.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).\
        filter(lambda x: len(x[1]) >= filterThreshold).map(lambda x: sorted(set(x[1]))).persist()
    data.unpersist()
    totalBaskets = userBusiness.count()
    chunks = userBusiness.glom().collect()
    localCand = []

    for chunk in chunks:
        cand = multiHash(chunk)
        localCand.append(cand)

    candidates = flatten(localCand)
    frequent = userBusiness.map(lambda x: isCand(x, candidates)).flatMap(lambda x: x).reduceByKey(add).\
        filter(lambda x: x[1] >= support).keys().collect()

    writeOutput(candidates, frequent)



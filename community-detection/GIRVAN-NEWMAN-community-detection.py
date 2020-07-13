from pyspark import SparkContext
from collections import defaultdict
from itertools import combinations
from time import time
import sys


class Graph:

    def __init__(self):

        self.levels = defaultdict(set)
        self.parents = defaultdict(set)
        self.nodes = defaultdict()
        self.shortest = defaultdict()

    def addNode(self, level, nodeName):

        self.levels[level].add(nodeName)
        self.nodes[nodeName] = [level, 1]
        self.shortest[nodeName] = 0

    def addParent(self, child, parent):

        self.parents[child].add(parent)
        self.shortest[child] += self.shortest[parent]

    def getLevel(self):

        return len(self.levels) - 1


def writeBetweenness(betweenness):

    with open(betweennessOutputFilepath, 'w') as f:
        for k, v in sorted(betweenness.items(), key = lambda x: -x[1]):
            f.write(str(k) + ", " + str(v/2) + "\n")


def bfs(root, connection):

    graph = Graph()
    level = 1

    graph.addNode(level, root)
    graph.shortest[root] = 1
    newLevel = True

    while newLevel:

        parents = graph.levels[level]
        level += 1

        for parent in parents:
            for child in connection[parent]:
                if child in graph.nodes.keys():
                    if graph.nodes[child][0] < level:
                        pass
                    else:
                        graph.addParent(child, parent)
                else:
                    graph.addNode(level, child)
                    graph.addParent(child, parent)

        if len(graph.levels[level]):
            pass
        else:
            newLevel = False
            level -= 1

    return graph


def getBetweenness(adj):

    store = defaultdict(float)

    for root in adj.keys():
        graph = bfs(root, adj)
        level = graph.getLevel()

        while level > 0:

            children = graph.levels[level]

            for child in children:
                parents = graph.parents[child]
                shortest = graph.shortest[child]

                for parent in parents:
                    edge = tuple(sorted((child, parent)))
                    edgeScore = graph.nodes[child][1] * (graph.shortest[parent] / shortest)
                    store[edge] += edgeScore
                    graph.nodes[parent][1] += edgeScore

            level -= 1

    return store


def getCommunities(newAdj):

    communities = []
    nodes = list(newAdj.keys())
    while(nodes):

        root = nodes[0]
        community = bfs(root, newAdj)
        visited = community.nodes.keys()
        for n in visited:
            nodes.remove(n)
        communities.append(community)

    return communities


def getModularity(communities):

    global m
    global adjacent

    modularity = 0
    for community in communities:

        nodes = community.nodes.keys()

        for i in nodes:
            for j in nodes:
                if i == j:
                    pass
                else:
                    ki = len(list(adjacent[i]))
                    kj = len(list(adjacent[j]))
                    if j in adjacent[i] or i in adjacent[j]:
                        modularity += (1 - ((ki * kj) / (2 * m)))
                    else:
                        modularity -= ((ki * kj) / (2 * m))

    return modularity / (2 * m)


def writeCommunities(bestCut):

    result = sorted([sorted(x.nodes.keys()) for x in bestCut], key = lambda x: (len(x), x))
    with open(communityOutputFilepath, 'w') as f:
        for res in result:
            line = str(res).strip("[]") + "\n"
            f.write(line)


if __name__ == "__main__":

    start = time()
    filterThreshold = 7#int(sys.argv[1])
    inputFilepath = 'ub_sample_data.csv'#sys.argv[2]
    betweennessOutputFilepath = 'task2betweennes'#sys.argv[3]
    communityOutputFilepath = 'task2community'#sys.argv[4]

    SparkContext.setSystemProperty('spark.executor.memory','4g')
    SparkContext.setSystemProperty('spark.driver.memory','4g')
    sc = SparkContext(master = "local[*]", appName = "hw4task2")
    sc.setLogLevel("ERROR")

    data = sc.textFile(inputFilepath).map(lambda line: line.split(","))
    header = data.first()
    data = data.filter(lambda x: x != header)
    ub = sc.broadcast(data.groupByKey().mapValues(set).filter(lambda x: len(x[1]) >= 7).collectAsMap())
    users = ub.value.keys()

    edges = sc.parallelize(combinations(ub.value, 2)).filter(lambda x: len(ub.value[x[0]] & ub.value[x[1]]) >= filterThreshold).persist()
    allEdges = edges.union(edges.map(lambda x: (x[1], x[0])))
    adjacent = allEdges.groupByKey().mapValues(set).collectAsMap()

    betweenness = getBetweenness(adjacent)
    writeBetweenness(betweenness)

    maxBList = []
    m = edges.count()
    bestQ = -1

    while (betweenness):

        maxB = max(betweenness, key = betweenness.get)
        maxBList.append(maxB)
        maxBList.append((maxB[1], maxB[0]))
        newEdges = allEdges.filter(lambda x: x not in maxBList)
        newAdj = newEdges.groupByKey().mapValues(set).collectAsMap()

        communities = getCommunities(newAdj)
        modularity = getModularity(communities)

        if modularity > bestQ:
            bestQ = modularity
            bestCut = communities

        betweenness = getBetweenness(newAdj)
        print(newEdges.count(), modularity, bestQ)

        if time() - start > 200:
            break

    writeCommunities(bestCut)







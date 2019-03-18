# why betweenness /=2
from pyspark import SparkContext
from pyspark import SparkConf
from itertools import islice
import time
import sys

st = time.time()
sc = SparkContext(appName="Ass4 task1")

infile = sys.argv[1]

rawdata = sc.textFile(infile).map(lambda line: line.split(","))

moviesGroupedByUserRDD = rawdata.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
								.map(lambda line: (int(line[0]), int(line[1]))).groupByKey()  # (u, [movies])

usersCombinations = moviesGroupedByUserRDD.cartesian(moviesGroupedByUserRDD).filter(lambda u : u[0][0]!=u[1][0])\
								 .map(lambda x: ((x[0][0], x[1][0]), set(x[0][1]).intersection(set(x[1][1]))))\
								 .filter(lambda x: len(x[1])>=9)\
								 .map(lambda x: (min(x[0][0], x[0][1]), max(x[0][0], x[0][1])))\
								 .distinct()\
								 .collect()

edgesBTWDictionary = {}
for edge in usersCombinations:
	edgesBTWDictionary[edge] = 0.0

import networkx as nx
from networkx import number_connected_components
from networkx import connected_components
import matplotlib.pyplot as plt

G = nx.Graph()
G.add_edges_from(usersCombinations)



def bfs(root):
	parents = dict()
	depth = dict()
	pathcount = dict()
	visited = []

	q = [root]
	depth[root] = 0
	pathcount[root] = 1
	parents[root] = []

	while(q):
		curr = q.pop(0)
		visited.append(curr)

		for node in G[curr]:

			if node not in depth:
				q.append(node)
				depth[node] = depth[curr]+1

			if depth[node] > depth[curr]:
				pathcount[node] = pathcount.get(node, 0) + pathcount[curr]
				parents[node] = parents.get(node,[])
				parents[node].append(curr)

	return parents, depth, pathcount, visited


def calculateBetweenness(root):

	parents, depth, pathcount, visited = bfs(root)
	weightFromEdgesBelow = dict()

	while(visited):
		node = visited.pop()
		w = (1 + weightFromEdgesBelow.get(node, 0))/float(pathcount[node])
		for parent in parents[node]:
			overallweight = w * pathcount[parent]
			a = min(node, parent)
			b = max(node, parent)
			edge = (a,b)
			edgesBTWDictionary[edge] += overallweight
			weightFromEdgesBelow[parent] =  weightFromEdgesBelow.get(parent,0) + overallweight

	return edgesBTWDictionary


for root in G:
	calculateBetweenness(root)

for key in edgesBTWDictionary:
	edgesBTWDictionary[key] /= 2

print edgesBTWDictionary

btw = ""
for key in sorted(edgesBTWDictionary.iterkeys()):
	btw += "("+str(key[0]) + ", " + str(key[1]) + ", "
	btw += str(edgesBTWDictionary[key]) + ")\n"

print btw

outputfileloc = "Surbhi_Batra_Betweenness.txt"
outputfile = open(outputfileloc, "w")
outputfile.write(btw)

btwtime = str(time.time() - st)


def calcA():
	A = {}
	for e in G.edges_iter():
		print e




print "btwtime", btwtime
print "endtime", str(time.time() - st)







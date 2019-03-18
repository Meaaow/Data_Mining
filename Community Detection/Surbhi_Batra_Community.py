from pyspark import SparkContext
from pyspark import SparkConf
from itertools import islice
import time
import sys

st = time.time()
sc = SparkContext('local',appName="Ass4 task2")

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
from networkx import connected_components

G = nx.Graph()
G.add_edges_from(edgesBTWDictionary)
# Gcopy = nx.Graph()
# Gcopy.add_edges_from(usersCombinations)

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


btw = ""
for key in sorted(edgesBTWDictionary.iterkeys()):
	btw += "("+str(key[0]) + ", " + str(key[1]) + ", "
	btw += str(edgesBTWDictionary[key]) + ")\n"



btwtime = str(time.time() - st)

# #####################################################################################

rankedBTW = sorted(edgesBTWDictionary, key = edgesBTWDictionary.get, reverse = 1)
for i in sorted(edgesBTWDictionary.keys()):
    edgesBTWDictionary[i] = (edgesBTWDictionary[i]//0.001)/1000

# rankedBTW = sorted(edgesBTWDictionary, key = edgesBTWDictionary.get, reverse = 1)



def calcA():
	A = {}
	for i in range(1,671):
		for j in range(i+1, 672):
			edge = (i,j)
			if edgesBTWDictionary.get(edge):
				A[edge] = 1
			else:
				A[edge] = 0
	return A

def calcDeg():
	deg = {}
	for i in G:
		deg[i] = G.degree(i)
	return deg

def calcQ():
	global nocurrComponents
	global m
	global blahcount
	global maxQ
	global maxcomponents
	global maxno
	q = 0
	comm = connected_components(Gcopy)
	commlist = list(comm)
	nonewComponents=len(commlist)
	# nonewComponents=len(commlist)
	if nonewComponents>nocurrComponents:
		nocurrComponents = nonewComponents
		for component in commlist:
			componentlist = sorted(list(component))
			for i in range(len(componentlist)-1):
				for j in range(i+1, len(componentlist)):
					node_i = componentlist[i]
					node_j = componentlist[j]
					a = A[(node_i, node_j)]
					ki = deg[node_i]
					kj = deg[node_j]
					qcurr = (a - ((ki*kj))/float(2*m))
					q += qcurr
					blahcount += 1
	Q = q/float(2*m)
	if Q>=maxQ:
		maxQ = Q
		maxcomponents = commlist
		maxno = nonewComponents
	return maxQ, maxcomponents,maxno

Gcopy = nx.Graph()
Gcopy.add_edges_from(edgesBTWDictionary)
A = calcA()
deg = calcDeg()
m = len(edgesBTWDictionary)
blahcount = 0
maxQ = 0
maxcomponents = []
maxno = 1
looptime = time.time()
nocurrComponents = 1
edgecount = 0

btwAndEdges = {}
for k, v in edgesBTWDictionary.iteritems():
    btwAndEdges.setdefault(v, []).append(k)



sortededges = sorted(btwAndEdges.keys(), reverse = 1)


for val in sortededges:
	edgecount+=1
	deletetheseedges = btwAndEdges[val]
	Gcopy.remove_edges_from(deletetheseedges)
	maxQ, maxcomponents,maxno = calcQ()



print "******** max components"
print str(maxcomponents)
print "maxQ", str(maxQ)
print "maxno", str(maxno)


print "btwtime", btwtime
print "looptime", str(time.time()-looptime)
print "endtime", str(time.time() - st)


print "m", m

outputfileloc3 = "Surbhi_Batra_Community.txt"
outputfile3 = open(outputfileloc3, "w")
for i in sorted(list(maxcomponents)):
	outputfile3.write(str(sorted(list(i)))+"\n")
outputfile3.close()

print "blahcount", str(blahcount)

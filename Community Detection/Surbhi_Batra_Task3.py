 # sudo python -m pip install community
# sudo python -m pip install python-louvain

from networkx import edge_betweenness_centrality
from random import random
from pyspark import SparkContext
from pyspark import SparkConf
from itertools import islice
import time
import sys

import networkx as nx
from networkx import number_connected_components
from networkx import connected_components
import community
# from networkx.algorithms.community import best_partition
from community import community_louvain
import community.community_louvain as community

st = time.time()
sc = SparkContext('local',appName="Ass4 task3")

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

# import matplotlib.pyplot as plt

G = nx.Graph()
G.add_edges_from(usersCombinations)



partition = community.best_partition(G)
print "*******************"

communities = {}
for k, v in partition.iteritems():
    communities.setdefault(v, []).append(k)

# print communities
s = ""
for k in sorted(communities):
	for val in sorted(communities[k]):
		if  val == communities[k][0]:
			s += "["+str(val)+", "
		elif val == communities[k][-1]:
			s += str(val) + "]\n"
		else:
			s += str(val) + ", "
	s += "\n"

print s

mod = community.modularity(partition,G)
print mod

outputfileloc = "Surbhi_Batra_Task3.txt"
outputfile = open(outputfileloc, "w")
outputfile.write(s)
outputfile.close()







print time.time()-st

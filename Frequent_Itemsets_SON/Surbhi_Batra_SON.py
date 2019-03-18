from pyspark import SparkContext
from pyspark import SparkConf
from itertools import islice
import sys
import itertools
from numpy import *
from itertools import groupby
import time
from math import floor, ceil
from collections import Counter

start_time = time.time()	

caseno = sys.argv[1]
infile = sys.argv[2]
minimumsupport = int(sys.argv[3])

filename1 = infile.split(".")
filename2 = filename1[:len(filename1)-1]
filename = filename2[0]
for f in filename2[1:]:
    filename+="."+f

outfileloc = "Surbhi_Batra_SON_"+filename+".case"+caseno+"-"+str(minimumsupport)+".txt"
outfile = open(outfileloc, 'w')

conf = SparkConf().setMaster('local[8]').setAppName('SON app')
# conf = SparkConf().setAppName('task1')

sc =SparkContext(conf=conf)
		
def createSingles(databaskets):
	counts = Counter(x for xx in databaskets for x in set(xx))
	candidateList  = []
	for key in sorted(counts.iterkeys()):
		candidateList.append([key])
	candidateList = list(map(frozenset,candidateList))
	# print(time.time())
	return candidateList


def filterL(databaskets, ck, minSupport):
	# databasket : list of sets ; ck: list of frozen sets
	# print("filter")
	# print(time.time())
	freqItemsList = []
	countItems = dict()
	for bask in databaskets:
		for cand in ck:
			if(cand.issubset(bask)):
				countItems[cand] = countItems.get(cand,0) + 1
	for key in countItems:
		if(countItems[key] >= minSupport):
			freqItemsList.insert(0,key)
	return freqItemsList

def genSubsets(corrCand,k):
	# print("generate subsets")
	# print(time.time())
	retSubsetList = []
	corrCandLen = len(corrCand)
	for i in range(corrCandLen):
		for j in range(i+1, corrCandLen):
			lista = list(corrCand[i])[:k-2]
			listb = list(corrCand[j])[:k-2]
			lista.sort();
			listb.sort();
			if(lista==listb):
				retSubsetList.append(corrCand[i]|corrCand[j])
			# seta = set(corrCandLen[i])
			# setb = set(corrCandLen[j])
			# setc = (seta|setb)
			# if(len(setc)==k)
			# 	retSubsetList.append(corrCand[i]|corrCand[j])
	# if(k==5):
	# 	print(retSubsetList)
	return retSubsetList;


def apriori(totalBasketsInData):
	# print("~~~~~~~~~~~~apriori~~~~~~~~~~~~~~~~~~~~")
	# print(time.time() - start_time)
	def _apriori(baskets2):
		baskets = list(baskets2)
		# print("imap converted to list")
		# print(time.time() - start_time)
		basketsInChunk = len(baskets)
		# print("basketsinchunk:")
		# print(basketsInChunk)
		# print(time.time() - start_time)
		minSup = minimumsupport*basketsInChunk/float(totalBasketsInData)
		minSup = int((minSup))
		print("minsup: ")	
		print(minSup)
		k=2
		c1 = createSingles(baskets)
		# print("singles created")
		# print(time.time() - start_time)
		basketSet = list(map(set, baskets))
		L1 = filterL(basketSet, c1, minSup)
		L = [L1]
		# print("L1 created")
		print(time.time() - start_time)
		while(len(L[k-2])>0):
			ck = genSubsets(L[k-2],k)
			# print("ck created of length " + str(len(ck)) + " and k is " + str(k))
			# print(time.time() - start_time)
			lk = filterL(basketSet, ck, minSup)	
			L.append(lk)
			# print("L created")
			# print(time.time() - start_time)
			k = k+1
		# for tt in L:
		# 	if(missingele in tt):
		# 			print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		# 			print("llllllllllllllllllllllllllllllllllllllllllllllllllllllllllll")
		# 			print("____________________________________________________________")
		# 			print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")	
		unfreezeL = [tuple(item) for sublist in L for item in sublist]
		# print("unfreezeL created")
		# print(time.time() - start_time)
		return unfreezeL
	return _apriori

def countCandidatePairs(candidateItems2):
	# print("count candidates")
	# print(time.time())
	candidateItems =  list(set(tuple(sorted(t)) for t in candidateItems2)) #veryImpt : removes duplicates : do not touch
	candidateItems = [frozenset(x) for x in candidateItems]
	def _countCandidatePairs(databaskets2):
		databaskets3 = list(databaskets2)
		databaskets = [set(x) for x in databaskets3]
		countItems  = dict()
		for bask in databaskets:
			for cand in candidateItems:
				if(cand.issubset(bask)):
					countItems[cand] = countItems.get(cand,0) + 1
		return countItems.iteritems()
	return _countCandidatePairs

#########################################################################

res = []

if caseno=='1':
	data = sc.textFile(infile) \
			.map(lambda line: line.split(",")) \
			.filter(lambda line: len(line)>1) \
			.map(lambda line: (line[0], line[1])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list) \
			.map(lambda x: (x[1]))

elif caseno=='2':
	data = sc.textFile(infile) \
			.map(lambda line: line.split(",")) \
			.filter(lambda line: len(line)>1) \
			.map(lambda line: (line[1], line[0])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list) \
			.map(lambda x: (x[1]))

if (minimumsupport < 6):
	data = data.repartition(1)


partitions = data.getNumPartitions()
print("*** partitions :")
print(partitions)
# adjustedsupport = (float(len(basketsInChunk))/float(len(data))))*supportGlobal
# adjustedsupport  = (minimumsupport)/partitions
# print("**** adsted support :")
# print(adjustedsupport)
totalBasketsInData = data.count()
print("total:")
print(totalBasketsInData)

First_Map = data.mapPartitions(apriori(totalBasketsInData))
First_Map.collect()
First_Reduce = First_Map.map(lambda s : (s,1)).reduceByKey(lambda x,y : x)
intermediate = First_Reduce.keys().collect()
Second_Map = data.mapPartitions(countCandidatePairs(intermediate))
Second_Reduce = Second_Map.reduceByKey(lambda x,y : x+y).filter(lambda x : x[1] >= minimumsupport)
ans = Second_Reduce.keys().collect()
unfreezeAns = [list(x) for x in ans]
b = [sorted(i) for i in unfreezeAns]
c = sorted(sorted(b),key = len)
tupleans = [tuple(x) for x in c]
grpAns = [list(g[1]) for g in groupby(sorted(tupleans, key=len), len)]



ansString = ""
for i in grpAns:
	for jj in range(len(i)):
		j = i[jj]
		j = list(j)
		ansString += "("
		for k in j:
			if k== j[len(j)-1]:
				ansString += str(k)
			else:
				ansString += str(k)+", "
		if jj == len(i)-1:
			ansString += ")"
		else :
			ansString += "), "
	ansString += "\n\n"


outfile.write(ansString)

print("********** Final Ans ****************:")
print(ansString)
print("total time {}".format(time.time() - start_time))
print("len :")
print(len(c)) 
print("SUCCESSss")
print(partitions)


# intunfreezeAns = [list(x) for x in intermediate]
# intb = [sorted(i) for i in intunfreezeAns]
# intc = sorted(sorted(intb),key = len)
# inttupleans = [tuple(x) for x in intc]
# intermediate2 = [list(g[1]) for g in groupby(sorted(inttupleans, key=len), len)]

# outfile2 = open('intermediate.txt', 'w')
# outfile2.write(str(intermediate2))


#Prb 1
 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 1 Small2.csv 3
 # total time  77.0368101597 s  partitions =2  local[2] or local[*]
 # total time  36.9639399052 s  partitions =1  local[1]	
 # len : 11300


 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 2 Small2.csv 5
 # total time  11.4254529476 partitions =1 local[1]
 # total time   partitions =1 local[*]	
 # len : 5446


#Prb2
 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 1 MovieLens.Small.csv 120
 # total time  6.18635201454 partitions =1
 #  len: 591

 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 1 MovieLens.Small.csv 150
 # total time  5.52492785454 partitions =2
 #  len :144


 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 2 MovieLens.Small.csv 180
 # total time 36.2949130535 partitions = 2 
 # len : 3453

 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 2 MovieLens.Small.csv 200
 # total time 22.7473111153 if partitions =2
 # len : 2137


# Problem 3

 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 1 MovieLens.Big.csv 30000
  #  total time : total time 275.285174847 partitions = 16
  # len : 207
  # (1), (32), (34), (47), (50), (110), (150), (153), (165), (231), (260), (296), (316), (318), (344), (356), (364), (367), (377), (380), (457), (480), (500), (527), (541), (588), (589), (590), (592), (593), (595), (597), (608), (648), (733), (736), (780), (858), (1036), (1097), (1136), (1196), (1197), (1198), (1210), (1214), (1240), (1265), (1270), (1291), (1580), (1721), (2028), (2571), (2762), (2858), (2959), (3578), (4226), (4306), (4993), (5952), (7153)

  # (1, 260), (1, 296), (1, 356), (1, 480), (32, 296), (47, 296), (47, 318), (47, 356), (47, 480), (47, 593), (50, 296), (50, 318), (50, 356), (50, 593), (110, 150), (110, 296), (110, 318), (110, 356), (110, 457), (110, 480), (110, 589), (110, 593), (150, 296), (150, 318), (150, 356), (150, 380), (150, 457), (150, 480), (150, 590), (150, 592), (150, 593), (260, 296), (260, 318), (260, 356), (260, 480), (260, 589), (260, 593), (260, 780), (260, 1196), (260, 1198), (260, 1210), (260, 2571), (296, 318), (296, 356), (296, 377), (296, 380), (296, 457), (296, 480), (296, 527), (296, 589), (296, 590), (296, 592), (296, 593), (296, 608), (296, 2571), (296, 2858), (318, 356), (318, 457), (318, 480), (318, 527), (318, 589), (318, 590), (318, 592), (318, 593), (318, 2571), (344, 356), (356, 364), (356, 377), (356, 380), (356, 457), (356, 480), (356, 500), (356, 527), (356, 588), (356, 589), (356, 590), (356, 592), (356, 593), (356, 780), (356, 2571), (364, 480), (377, 457), (377, 480), (377, 589), (377, 593), (380, 457), (380, 480), (380, 589), (380, 592), (380, 593), (457, 480), (457, 589), (457, 590), (457, 592), (457, 593), (480, 527), (480, 588), (480, 589), (480, 590), (480, 592), (480, 593), (480, 780), (480, 2571), (527, 593), (589, 592), (589, 593), (590, 592), (590, 593), (592, 593), (593, 2571), (1196, 1198), (1196, 1210), (1196, 2571), (2571, 2762), (2571, 2858), (2571, 2959), (4993, 5952)

  # (110, 296, 356), (110, 296, 593), (110, 356, 480), (110, 356, 593), (150, 356, 480), (260, 1196, 1210), (296, 318, 356), (296, 318, 480), (296, 318, 593), (296, 356, 457), (296, 356, 480), (296, 356, 589), (296, 356, 593), (296, 457, 593), (296, 480, 589), (296, 480, 593), (296, 589, 593), (318, 356, 480), (318, 356, 593), (356, 377, 480), (356, 457, 480), (356, 457, 593), (356, 480, 589), (356, 480, 592), (356, 480, 593), (356, 589, 593), (480, 589, 593)


 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 1 MovieLens.Big.csv 35000
 # total time : 261.426958084 partitions = 16	
 # len : 79
 # (1), (32), (47), (50), (110), (150), (260), (296), (318), (344), (356), (364), (377), (380), (457), (480), (527), (588), (589), (590), (592), (593), (595), (608), (648), (780), (858), (1196), (1198), (1210), (1270), (1580), (2028), (2571), (2762), (2858), (2959), (4993)

 # (47, 296), (50, 296), (50, 318), (110, 296), (110, 318), (110, 356), (110, 480), (110, 593), (150, 296), (150, 356), (260, 1196), (260, 1210), (296, 318), (296, 356), (296, 457), (296, 480), (296, 527), (296, 589), (296, 592), (296, 593), (318, 356), (318, 480), (318, 527), (318, 593), (356, 457), (356, 480), (356, 527), (356, 589), (356, 593), (377, 480), (457, 480), (457, 593), (480, 589), (480, 592), (480, 593), (527, 593), (589, 593), (1196, 1210)

 # (296, 318, 593), (296, 356, 480), (296, 356, 593)



 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 2 MovieLens.Big.csv 2800
 # 347.685868979 seconds 16 local *
 # 153

 # $SPARK_HOME/bin/spark-submit Surbhi_Batra_SON.py 2 MovieLens.Big.csv 3000
 # total time 345.301845074
 # len : 109

print "Done with processing!"

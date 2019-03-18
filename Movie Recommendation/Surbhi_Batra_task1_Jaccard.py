# $SPARK_HOME/bin/spark-submit Surbhi_Batra_task1_Jaccard.py MovieLens.Small.csv 

from pyspark import SparkConf, SparkContext
import sys
from itertools import islice
from numpy import inf
import time
import json

st = time.time()
sc = SparkContext(appName="Ass3")

infile = sys.argv[1]

m  = 671
bands = 6
rows = 2


rawdata = sc.textFile(infile) \
			.map(lambda line: line.split(","))

data = rawdata.map(lambda line: (line[0], line[1])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.groupByKey()\
			.mapValues(list)

movieList = rawdata.map(lambda line: (line[1], line[0])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.groupByKey()\
			.mapValues(list).sortByKey()\
			.collect() ###do not touch!!!!!!!!!
			#contains users for each movie

def createHashFxs():
	uf = []

	uf.append(data.map(lambda x:((11*x[0]+31)%m)).collect())
	uf.append(data.map(lambda x:((131*x[0]+7)%m)).collect())
	uf.append(data.map(lambda x:((173*x[0]+13)%m)).collect())
	uf.append(data.map(lambda x:((17*x[0]+11)%m)).collect())
	uf.append(data.map(lambda x:((7*x[0]+19)%m)).collect())
	uf.append(data.map(lambda x:((113*x[0]+311)%m)).collect())
	uf.append(data.map(lambda x:((53*x[0]+731)%m)).collect())
	uf.append(data.map(lambda x:((193*x[0]+387)%m)).collect())
	uf.append(data.map(lambda x:((117*x[0]+3317)%m)).collect())
	uf.append(data.map(lambda x:((13*x[0]+37)%m)).collect())
	uf.append(data.map(lambda x:((19*x[0]+73)%m)).collect())
	uf.append(data.map(lambda x:((31*x[0]+37)%m)).collect())

	return uf

def createSignDict(keys, listSize):
	for i in keys:
		signDict[i] = []
		for j in range(listSize):
			signDict[i].append(float(inf))


def updateDict(x,y):
	key = x[0]
	value = x[1]
	signDict[key][y] = value

def isCandidate(item1, item2):
	for band in range(bands):
		count = 0;
		for row in range(rows):
			if signDict[item1][(rows*band)+row] == signDict[item2][(rows*band)+row]:
				count+=1;
				if(count == rows):
					return 1
			else : continue
	return 0

def generateCandidates(items):
	candidates = []
	for i1 in range(len(items)):
		for i2 in range(i1+1,len(items)):
			if isCandidate(items[i1],items[i2]):
				candidates.append((items[i1],items[i2]))
	return candidates

def calcJaccardSimilarity(candidates):
	movieDict = {}
	for movie in movieList:
		movieDict[movie[0]] = movie[1]
	J = []
	for candidate in candidates:
		c1 = candidate[0]
		c2 = candidate[1]
		c11 = min(int(c1), int(c2))
		c22 = max(int(c1), int(c2))
		c1List = movieDict[c1]
		c2List = movieDict[c2]
		match = 0
		for i in c1List:
			for j in c2List:
				if i==j:
					match+=1
				elif i>j:
					j+=1
				else : i+=1
		j = match/float(len(c1List)+len(c2List)-match)
		if j>=0.5:
			ele = tuple((c11,c22,j))
			J.append(ele)
	return J



movies = data.flatMap(lambda x: x[1]).distinct().collect() #contains list of unique movies
uf = createHashFxs()
noOfHashfxs = len(uf)
signDict = {}
createSignDict(movies, noOfHashfxs)


for hashfx in range(len(uf)):
	movieNpossiblehashRDD = data.map(lambda x: (x[0], x[1], uf[hashfx][x[0]-1])).map(lambda x: (x[1], x[2])).flatMap(lambda x: ((y, x[1]) for y in x[0]))
	movieNminimumhashRDD=movieNpossiblehashRDD.groupByKey().map(lambda x: (x[0], min(list(x[1]))))
	movieNminimumhash = movieNminimumhashRDD.collect()

	for pair in movieNminimumhash:
		updateDict(pair,hashfx)

midtime1 = time.time()-st
candidates = generateCandidates(movies);
midtime2 = time.time()-st
result = calcJaccardSimilarity(candidates)
midtime3 = time.time() - st

finalRes = sorted(result)

ii=0;
similarMovieString = ""
for mmr in finalRes:
	# print ii
	for ind in range(len(mmr)):
		if(ii==len(finalRes)-1):
			if(ind==len(mmr)-1):
				similarMovieString += str(mmr[ind])
			else:
				similarMovieString += str(mmr[ind])+", "
		else:
			if(ind==len(mmr)-1):
				similarMovieString += str(mmr[ind])+"\n"
			else:
				similarMovieString += str(mmr[ind])+", "
	ii +=1

outfilename = "Surbhi_Batra_SimilarMovies_Jaccard.txt"
outfile = open(outfilename,'w')
outfile.write(similarMovieString)
outfile.close()

# print("length.. candidates")
# print(len(candidates))
# print("len.. final res")
# print(len(finalRes))

# print("mid time")
# print(midtime1)
# print(midtime2)
# print(midtime3)
print("end of lsh time")
print(time.time()-st)



# ####################################################### PRECISON AND RECALL ###########################################################


# groundtruthfile = open("SimilarMovies.GroundTruth.05.csv", 'r')
# lines_gt = groundtruthfile.readlines()

# mySimilarMovies = set()
# gtSimilarMovies = set()

# strings = similarMovieString.split('\n')
# for line in strings:
# 	movie1, movie2, rating = line.strip().split(',')
# 	movie1 = movie1.strip()
# 	movie2 = movie2.strip()
# 	mySimilarMovies.add((movie1,movie2))


# for line in lines_gt:
# 	movie1, movie2 = line.strip().split(',')
# 	movie1 = movie1.strip()
# 	movie2 = movie2.strip()
# 	gtSimilarMovies.add((movie1,movie2))

# tp = mySimilarMovies & gtSimilarMovies
# fp = mySimilarMovies.difference(gtSimilarMovies)
# fn = gtSimilarMovies.difference(mySimilarMovies)

# lentp = len(tp)
# lenfp = len(fp)
# lenfn = len(fn)

# precision = lentp/float(lentp+lenfp)
# recall = lentp/float(lentp+lenfn)


# print "\nprecision: %f" %precision
# print "\nrecall: %f" %recall

# print(time.time()-st)
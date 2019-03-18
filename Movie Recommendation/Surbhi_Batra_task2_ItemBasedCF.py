from pyspark import SparkConf, SparkContext
import sys
from itertools import islice
from numpy import inf
import time
import json
import math

st = time.time()
sc = SparkContext(appName="Ass3_item")

allFile = sys.argv[1]
testFile = sys.argv[2]
m  = 671
bands = 6
rows = 2

debug = ""
level0 = 13937
level1 = 4878
level2 = 1211
level3 = 218
level4 = 12	
rmsethresh = 1.039897994

w_alldata = sc.textFile(allFile).map(lambda x: x.split(','))\
			.mapPartitionsWithIndex(lambda idx, it: islice(it,1,None) if idx==0 else it)\
			.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).sortByKey()

w_testData = sc.textFile(testFile).map(lambda x: x.split(','))\
			.mapPartitionsWithIndex(lambda idx, it: islice(it,1,None) if idx==0 else it)\
			.map(lambda x: (int(x[0]), int(x[1]))).sortByKey()

w_testDataTemp = w_testData.map(lambda x: ((x[0], x[1]),1))

w_trainingData = w_alldata.subtractByKey(w_testDataTemp).map(lambda x: (x[0][0], x[0][1], x[1])).map(lambda x:(x[0], x[1])).sortByKey()


# jaccard
data = w_trainingData.map(lambda line: (line[0], line[1])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.groupByKey()\
			.mapValues(list)

movieList = w_trainingData.map(lambda line: (line[1], line[0])) \
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
			.map(lambda xt : [int(x) for x in xt]) \
			.groupByKey()\
			.mapValues(list).sortByKey()\
			.collect() ###do not touch!!!!!!!!!
			# contains users for each movie

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

# midtime1 = time.time()-st
candidates = generateCandidates(movies)
result = calcJaccardSimilarity(candidates)

JaccardWeights = sorted(result)


ii=0;
similarMovieString = ""
for mmr in JaccardWeights:
	# print ii
	for ind in range(len(mmr)):
		if(ii==len(JaccardWeights)-1):
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

outfilename = "weights_itembased.txt"
outfile = open(outfilename,'w')
outfile.write(similarMovieString)
outfile.close()

print("length.. candidates")
print(len(candidates))
print("len.. final res")
print(len(JaccardWeights))

print("Jaccard Weights Calculated")
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

# # length.. candidates
# # 529543
# # len.. final res
# # 211104
# # Jaccard Weights Calculated
# # 128.153980017

# # precision: 0.639562

# # recall: 0.351386
# # 129.016535997

# ##################################################################################################################################################
testData = sc.textFile(testFile).mapPartitionsWithIndex(lambda idx, it: islice(it,1,None) if idx==0 else it).map(lambda x: x.split(',')).map(lambda x: (int(x[0]), int(x[1]))).sortByKey()
testDataUser = testData.groupByKey().sortByKey()
testDataUserCollect = testDataUser.collect()
moviesDictTemp = w_alldata.subtractByKey(w_testDataTemp).map(lambda x: (x[0][1],(x[0][0], x[1]))).groupByKey().collect() # (m, (u,r))
trainingData = w_alldata.subtractByKey(w_testDataTemp).map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().sortByKey()
trainingDataList = trainingData.collect()

# WeightsFile = sc.parallelize(JaccardWeights).map(lambda x: x.split(',')).map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))

weightsfileloc = 'weights_itembased.txt'
# weightsfileloc = 'weights_chhotu.csv'
WeightsFile = sc.textFile(weightsfileloc).map(lambda x: x.split(',')).map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).collect()



Weights = {}
for w in WeightsFile:
	Weights[w[0]] = w[1]

print Weights


Predictions = []

# moviesDict = {}
# for mov in moviesDictTemp:
# 	testMov = mov[0]
# 	testMov_allusers = list(mov[1])
# 	moviesDict[testMov] = testMov_allusers

# print testDataUserCollect
for test in testDataUserCollect:
	testuser = test[0]
	testusertestmovies = list(test[1])
	# print test
	# print testuser
	# print testusertestmovies
	# print trainingDataList		

	testuserallmovies=list(trainingDataList[testuser-1][1])
	# print testuserallmovies

	# P_num = 0
	# P_den = 0
	

	for firstmov in testusertestmovies:
		num = 0
		den = 0
		for secondmr in testuserallmovies:
			secondmov = secondmr[0]
			secondrat = secondmr[1]
			a = min(firstmov, secondmov)
			b = max(firstmov, secondmov)
			# print a
			# print b
			if Weights.get((a,b)):
				w = Weights[(a,b)]
				num += secondrat*w
				den += abs(w)

		if den==0:
			p = 0
		else :
			p = num/float(den)

		if p >5:
			p =5
		if p<0:
			p=0

		Predictions.append((testuser,firstmov,p))


print Predictions


predictionsRDD = sc.parallelize(Predictions).map(lambda r: ((r[0], r[1]), r[2]))






everything = w_alldata.join(predictionsRDD)
n = everything.count()

RMSE = everything.map(lambda x: math.pow(x[1][0] - x[1][1], 2)).map(lambda x: (1,x)).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()
cd = everything.collect()
# a = w_alldata.take(5)
# b = predictionsRDD.take(5)
# print everything
# print n
# print RMSE
# print cd
# print a
# print b
res = pow(RMSE[0]/float(n), 0.5)

errorCalc = everything.map(lambda x: (x[0][0], x[0][1], x[1][0] - x[1][1]))
errorCalc2 = errorCalc.map(lambda x: (abs(x[2]), math.floor(abs(x[2]))))
errorCalc3 = errorCalc2.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y)
errorCalc4 = errorCalc3.map(lambda x: ((4, x[1]) if x[0]>=4 else x)).reduceByKey(lambda x,y: x+y).sortByKey()
e4 = errorCalc4.collect()

status = ["none", "none", "none", "none", "none", "none"]
if(int(e4[0][1])<level0):
	status[0] = " less"
else: status[0] = "more"

if(int(e4[1][1])<level1):
	status[1] = " less"
else: status[1] = "more"

if(int(e4[2][1]) <level2):
	status[2] = " less"
else: status[2] = "more"

if(int(e4[3][1])<level3):
	status[3] = " less"
else: status[3] = "more"

if(int(e4[4][1])<level4):
	status[4] = " less"
else: status[4] = "more"

if(res<rmsethresh):
	status[5] = " less"
else: status[5] = "more"

print(\
"\n >=0 and <1 : " + str(e4[0][1]) + status[0] +\
"\n >=1 and <2 : " + str(e4[1][1]) + status[1] +\
"\n >=2 and <3: " + str(e4[2][1]) + status[2] +\
"\n >=3 and <4 : " + str(e4[3][1]) + status[3] +\
"\n >=4	   : " + str(e4[4][1]) + status[4] +\
"\n \n RMSE : " + str(res) + status[5])



outputPred = sorted(Predictions)
outputPredString = ""
for umr in outputPred:
	for ind in range(len(umr)):
		if(ind==len(umr)-1):
			outputPredString += str(umr[ind])+"\n"
		else:
			outputPredString += str(umr[ind])+", "


outfile = open("Surbhi_Batra_ItemBasedCF.txt",'w')
outfile.write(outputPredString)
outfile.close()
print time.time()-st




 
#  >=0 and <1 : 800 less
#  >=1 and <2 : 1299 less
#  >=2 and <3: 2681more
#  >=3 and <4 : 6307more
#  >=4	   : 9169more
 
#  RMSE : 3.54410355144more
# 155.510996819





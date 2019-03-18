# $SPARK_HOME/bin/spark-submit Surbhi_Batra_task2_UserBasedCF.py MovieLens.Small.csv testing_small.csv

from pyspark import SparkConf, SparkContext
from itertools import islice
import time
import math
from math import sqrt
import sys

st = time.time()
sc = SparkContext(appName = "Ass3_Task2_User")

allFile = sys.argv[1]
testFile = sys.argv[2]
# allFile = "MovieLens.Small.csv"
# testFile = "testing_small.csv"

debug = ""
level0 = 13937
level1 = 4878
level2 = 1211
level3 = 218
level4 = 12	
rmsethresh = 1.039897994

def calcWeights(trainingDataList):
	weights = {}
	for i in range(len(trainingDataList)):
		for j in range(i+1,len(trainingDataList)):
			u = trainingDataList[i][0]
			v = trainingDataList[j][0]
			detailsU = sorted(list(trainingDataList[i][1]))
			detailsV = sorted(list(trainingDataList[j][1]))			
			ii=0
			jj=0
			ru=0
			rv=0
			indU = []
			indV = []
			while(ii<len(detailsU) and jj<len(detailsV)):
				if(detailsU[ii][0] == detailsV[jj][0]):
					ru += detailsU[ii][1]
					rv += detailsV[jj][1]
					indU.append(ii)
					indV.append(jj)
					ii+=1
					jj+=1
				elif(detailsU[ii][0] < detailsV[jj][0]):
					ii+=1
				else: 
					jj+=1

			w = 0
			if len(indU)!=0:
				rubar = ru/float(len(indU))
				rvbar = rv/float(len(indV))

				num = 0
				lden = 0
				rden = 0

				for coi in range(len(indU)):

					l = detailsU[indU[coi]][1]-rubar
					r = detailsV[indV[coi]][1]-rvbar


					num += (l*r)
					lden += pow(l,2)
					rden += pow(r,2)

				den = sqrt(lden)*sqrt(rden)

				if den!=0:
					w = num/float(den)
				else: 
					w = 0

			weights[(i+1,j+1)] = w

	return weights


alldata = sc.textFile(allFile).map(lambda x: x.split(','))\
			.mapPartitionsWithIndex(lambda idx, it: islice(it,1,None) if idx==0 else it)\
			.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).sortByKey()

alldatacollect = alldata.map(lambda x: (x[0],(x[1]))).collect()

debug += "\nall data: " + str(alldatacollect)

testData = sc.textFile(testFile).map(lambda x: x.split(','))\
			.mapPartitionsWithIndex(lambda idx, it: islice(it,1,None) if idx==0 else it)\
			.map(lambda x: (int(x[0]), int(x[1]))).sortByKey()

testDataTemp = testData.map(lambda x: ((x[0], x[1]),1))


testDataUser = testData.groupByKey()
testDataUserCollect = testDataUser.collect()
testUsers = testDataUser.keys()

trainingData = alldata.subtractByKey(testDataTemp).map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().sortByKey()
trainingDataList = trainingData.collect()
moviesDictTemp = alldata.subtractByKey(testDataTemp).map(lambda x: (x[0][1],(x[0][0], x[1]))).groupByKey().collect()

debug += "\ntest data " + str(testData.collect())
debug += "\ntest data temp " + str(testDataTemp.collect())
debug += "\ntestData user " + str(testDataUserCollect)
debug += "\ntest user " + str(testUsers.collect())
debug += "\ntraining data list " + str(trainingDataList)

# print testDataUser.mapValues(list).collectAsMap()

Predictions = []


#######################


weights = calcWeights(trainingDataList)

#######################

moviesDict = {}
for mov in moviesDictTemp:
	testMov = mov[0]
	testMov_allusers = list(mov[1])
	moviesDict[testMov] = testMov_allusers

P_rAbar = {}
P_rbar = {}

for test in testDataUserCollect:
	testuser = test[0]
	testusertestmovies = list(test[1])

	testuserallmovies=list(trainingDataList[testuser-1][1])
	sumA = 0
	rA_bar = 0
	c = 0

	for mov in testuserallmovies:
		sumA += mov[1]
		c += 1
	rA_bar = sumA/float(c)

	P_rAbar[testuser] = rA_bar

	P_num = 0
	P_den = 0
	
	
	for mov in testusertestmovies:
		if(moviesDict.get(mov)):
			usersWhoRatedThisMovie = moviesDict[mov]

			for user in usersWhoRatedThisMovie:
				r_user_testmov = user[1]

				trainingOfUser = list(trainingDataList[user[0]-1][1])

				sum_r = 0
				for item in trainingOfUser:
					
					if item[0]!=mov:
						sum_r += item[1]

				rbar = sum_r/float(len(trainingOfUser)-1)

				P_rbar[user[0]] = rbar

				userA = min(testuser,user[0])
				userB = max(testuser,user[0])

				P_num += (r_user_testmov - rbar)*weights[(userA, userB)]
				P_den += abs(weights[(userA, userB)])


		P = P_rAbar[testuser]

		if(P_den != 0):
			P = P + P_num/float(abs(P_den))

		if P==5:
			P =5
		if P<0:
			P=0

		Predictions.append((testuser, mov, P))

##############################

predictionsRDD = sc.parallelize(Predictions).map(lambda r: ((r[0], r[1]), r[2]))

everything = alldata.join(predictionsRDD)
n = everything.count()

RMSE = everything.map(lambda x: math.pow(x[1][0] - x[1][1], 2)).map(lambda x: (1,x)).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()
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

print "Time : ",time.time()-st

outputPred = sorted(Predictions)
outputPredString = ""
for umr in outputPred:
	for ind in range(len(umr)):
		if(ind==len(umr)-1):
			outputPredString += str(umr[ind])+"\n"
		else:
			outputPredString += str(umr[ind])+", "


outfile = open("Surbhi_Batra_UserBasedCF.txt",'w')
outfile.write(outputPredString)
outfile.close()
# f = open('weights_user','w')
# for i in weights:
#     f.write("%s" % str(i) + "," +"%s" % str(weights[i]))
#     f.write("\n")
    
# f.close()

# all data: [((1, 100), 4.0), ((1, 200), 4.0), ((1, 300), 5.0), ((1, 400), 5.0), ((2, 100), 4.0), ((2, 200), 2.0), ((2, 300), 1.0), ((3, 100), 3.0), ((3, 300), 2.0), ((3, 400), 4.0), ((4, 100), 4.0), ((4, 200), 4.0), ((5, 100), 2.0), ((5, 200), 1.0), ((5, 300), 3.0), ((5, 400), 5.0)]
# test data [(1, 200)]
# test data temp [((1, 200), 1)]
# training data list {
# 1: [(300, 5.0), (400, 5.0), (100, 4.0)], 
# 2: [(200, 2.0), (300, 1.0), (100, 4.0)], 
# 3: [(400, 4.0), (100, 3.0), (300, 2.0)], 
# 4: [(100, 4.0), (200, 4.0)], 
# 5: [(100, 2.0), (400, 5.0), (300, 3.0), (200, 1.0)]}
# testData user [(1, <pyspark.resultiterable.ResultIterable object at 0x7fe5cf4e1a10>)]
# test user [1]



#  >=0 and <1 : 15052FAIL !!!!!!!!
#  >=1 and <2 : 4160 success
#  >=2 and <3: 890 success
#  >=3 and <4 : 152 success
#  >=4	   : 2 success
 
#  RMSE : 0.962587911664 success
# 78.5888991356






# $SPARK_HOME/bin/spark-submit Surbhi_Batra_task2_UserBasedCF.py MovieLens.Small.csv testing_small.csv

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from itertools import islice
import time
import operator
import math
import sys

trainfile =  sys.argv[1]
testfile = sys.argv[2]


## USE THESE VALUES FOR ML-LATEST-SMALL
db = "small"
outfilename = "Surbhi_Batra_ModelBasedCF_Small.txt"
level0 = 13195
level1 = 5027
level2 = 1525
level3 = 407
level4 = 102
rmsethresh = 1.21686778
rank = 10
numIterations = 10
lambdas = 0.1

############################################

## USE THESE VALUES FOR ML-20million - BIG
# db = "big"

# outfilename = "output_task2_model_try6_big.txt"
# level0 = 3240397
# level1 = 707886
# level2 = 93517
# level3 = 12598
# level4 = 53
# rmsethresh = 0.83075011
# rank = 10
# numIterations = 10
# lambdas = 0.1

#############################################

st = time.time()
sc = SparkContext(appName="Ass3_Task2_Model")


alldata = sc.textFile(trainfile).map(lambda x: x.split(','))\
			.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
			.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).sortByKey() #[((1, 31), 2.5), ((1, 1029), 3.0), ((1, 1061), 3.0), ((1, 1129), 2.0), ((1, 1172), 4.0)]


testingdata_temp = sc.textFile(testfile).mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
				.map(lambda x: x.split(',')).map(lambda x: ((int(x[0]), int(x[1])),1)).sortByKey() 

trainingdata_temp = alldata.subtractByKey(testingdata_temp).sortByKey()

testingdata = testingdata_temp.map(lambda x: (x[0][0], x[0][1])) #[(1, 1172), (1, 1405), (1, 2193), (1, 2968), (2, 52)]
ngt = testingdata.count()
trainingdata = trainingdata_temp.map(lambda x: Rating(x[0][0], x[0][1], x[1])) #[Rating(user=1, product=31, rating=2.5), Rating(user=1, product=1029, rating=3.0), Rating(user=1, product=1061, rating=3.0), Rating(user=1, product=1129, rating=2.0), Rating(user=1, product=1263, rating=2.0)]


model = ALS.train(trainingdata, rank, numIterations,lambdas)

predictions = model.predictAll(testingdata).map(lambda r: ((r[0], r[1]), r[2]))
# .map(lambda r :(r[0], 5) if r[1]>=5 else r).map(lambda r :(r[0], 0) if r[1]<=0 else r)
everthing = alldata.join(predictions)
n = everthing.count()

RMSE = everthing.map(lambda x: math.pow(x[1][0] - x[1][1], 2)).map(lambda x: (1,x)).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()
res = pow(RMSE[0]/float(n), 0.5)

errorCalc = everthing.map(lambda x: (x[0][0], x[0][1], x[1][0] - x[1][1]))
errorCalc2 = errorCalc.map(lambda x: (abs(x[2]), math.floor(abs(x[2]))))
errorCalc3 = errorCalc2.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y)
errorCalc4 = errorCalc3.map(lambda x: ((4, x[1]) if x[0]>=4 else x)).reduceByKey(lambda x,y: x+y).sortByKey()
e4 = errorCalc4.collect()

outputPred = predictions.sortByKey().map(lambda x: (x[0][0], x[0][1], x[1]))
outputPredString = ""
for umr in outputPred.collect():
	for ind in range(len(umr)):
		if(ind==len(umr)-1):
			outputPredString += str(umr[ind])+"\n"
		else:
			outputPredString += str(umr[ind])+", "

outfile = open(outfilename,'w')
outfile.write(outputPredString)
outfile.close()

print db
print '\n No of rows in ground truth: %f' %ngt
print ' No of rows in Predictions: %f' %n 
print(rank, numIterations, lambdas)

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

print "\nEndtime: ", time.time() - st



#small
#  ngt: 20256.000000
#  n: 18733.000000
# (10, 10, 0.1)

#  >=0 and <1 : 13798more
#  >=1 and <2 : 4122 less
#  >=2 and <3: 704 less
#  >=3 and <4 : 103 less
#  >=4	   : 6 less
 
#  RMSE : 0.951102657209 less



# (10, 8, 0.35)

#  >=0 and <1 : 13072more
#  >=1 and <2 : 4993 less
#  >=2 and <3: 594 less
#  >=3 and <4 : 70 less
#  >=4	   : 4 less
 
#  RMSE : 0.971489486997 less




# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (10, 10, 0.1)

#  >=0 and <1 : 3226978 less
#  >=1 and <2 : 729790more
#  >=2 and <3: 81910 less
#  >=3 and <4 : 7443 less
#  >=4	   : 210more
 
#  RMSE : 0.818707576887 less

# Endtime:  1671.98184514

















###############################################################################################################################################


























######################## 	SUCCESS
#  ngt: 20256.000000
#  n: 18733.000000
# (5, 10, 0.1)

#  >=0 and <1 : 13945FAIL !!!!!!!!
#  >=1 and <2 : 3968 success
#  >=2 and <3: 692 success
#  >=3 and <4 : 120 success
#  >=4	   : 8 success
 
#  RMSE : 0.945473617852 success



# small
#  ngt: 20256.000000
#  n: 18733.000000

#  >=0 and <1 : 13088 success
#  >=1 and <2 : 4218 success
#  >=2 and <3: 1050 success
#  >=3 and <4 : 296 success
#  >=4	   : 81 success
 
#  RMSE : 1.12318626053 success

# Endtime:  17.4366869926

##########################################


# small

#  ngt: 20256.000000
#  n: 18733.000000
# (5, 10, 0.01)

#  >=0 and <1 : 12953 success
#  >=1 and <2 : 4346 success
#  >=2 and <3: 1069 success
#  >=3 and <4 : 278 success
#  >=4	   : 87 success
 
#  RMSE : 1.13470376926 success

# Endtime:  16.5447540283

##################################

# small

#  ngt: 20256.000000
#  n: 18733.000000
# (5, 10, 0.01)

#  >=0 and <1 : 12983 success
#  >=1 and <2 : 4267 success
#  >=2 and <3: 1089 success
#  >=3 and <4 : 310 success
#  >=4	   : 84 success
 
#  RMSE : 1.13779182877 success

# Endtime:  16.6939489841




############################################################################################################################

#################################################### FAIL  ################################################################

############################################################################################################################
#  small
#  ngt: 20256.000000
#  n: 18733.000000
# (5, 10, 0.1)

#  >=0 and <1 : 13928FAIL !!!!!!!!
#  >=1 and <2 : 3987 success
#  >=2 and <3: 703 success
#  >=3 and <4 : 105 success
#  >=4	   : 10 success
 
#  RMSE : 0.947585568781 success

# Endtime:  16.9096391201






# big 5,10,0.01 
# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (5, 10, 0.1)

#  >=0 and <1 : 3229782 success
#  >=1 and <2 : 720275FAIL !!!!!!!!
#  >=2 and <3: 87056 success
#  >=3 and <4 : 8942 success
#  >=4	   : 276FAIL !!!!!!!!
 
#  RMSE : 0.822967126506 success

# Endtime:  1732.47529101


# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (10, 10, 0.1)

#  >=0 and <1 : 3242479FAIL !!!!!!!!
#  >=1 and <2 : 712991FAIL !!!!!!!!
#  >=2 and <3: 82562 success
#  >=3 and <4 : 8079 success
#  >=4	   : 220FAIL !!!!!!!!
 
#  RMSE : 0.815138244792 success



# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (5, 10, 0.01)

#  >=0 and <1 : 3218105 success
#  >=1 and <2 : 725100FAIL !!!!!!!!
#  >=2 and <3: 91440 success
#  >=3 and <4 : 10637 success
#  >=4	   : 1049FAIL !!!!!!!!
 
#  RMSE : 0.834470664882FAIL !!!!!!!!

# Endtime:  1830.83243203

# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (12, 10, 0.1)

#  >=0 and <1 : 3224856 success
#  >=1 and <2 : 732296FAIL !!!!!!!!
#  >=2 and <3: 81631 success
#  >=3 and <4 : 7351 success
#  >=4	   : 197FAIL !!!!!!!!
 
#  RMSE : 0.819106578927 success

# Endtime:  1701.44375396


# big .........................

#  ngt: 4054451.000000
#  n: 4046331.000000
# (12, 8, 0.1)

#  >=0 and <1 : 3201220 success
#  >=1 and <2 : 756690FAIL !!!!!!!!
#  >=2 and <3: 81452 success
#  >=3 and <4 : 6797 success
#  >=4	   : 172FAIL !!!!!!!!
 
#  RMSE : 0.825352946829 success

# Endtime:  1907.44230294


# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (12, 7, 0.1)

#  >=0 and <1 : 3209742 success
#  >=1 and <2 : 747507FAIL !!!!!!!!
#  >=2 and <3: 81828 success
#  >=3 and <4 : 7070 success
#  >=4	   : 184FAIL !!!!!!!!
 
#  RMSE : 0.823465468238 success

# Endtime:  1704.24413919
##########################################
# big

#  ngt: 4054451.000000
#  n: 4046331.000000
# (12, 6, 0.1)

#  >=0 and <1 : 3197685 success
#  >=1 and <2 : 759674FAIL !!!!!!!!
#  >=2 and <3: 81906 success
#  >=3 and <4 : 6892 success
#  >=4	   : 174FAIL !!!!!!!!
 
#  RMSE : 0.827022133957 success

# Endtime:  1613.35705304

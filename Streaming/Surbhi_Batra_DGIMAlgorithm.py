from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
import math

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 9999)
sc.setLogLevel(logLevel = "off")
words = lines.flatMap(lambda line: line.split(" "))

mainarray = []
maindict = {}
windowlen = 1000
totalbuckets = 1+int(math.log(windowlen,2))
bucketlist = list()
timestamp = 0
actualcount = 0
foractualcount = []

for i in range(totalbuckets):
    bucketcap = int(math.pow(2,i))
    bucketlist.append(bucketcap)
    maindict[bucketcap] = list()



def dgim(arr):
    global timestamp
    global actualcount
    global foractualcount
    lenarr = len(arr)
    for currbit in arr:
        if len(foractualcount)<windowlen:
            foractualcount.append(currbit)
        else:
            foractualcount.append(currbit)
            foractualcount.pop(0)
        timestamp = (timestamp+1)%windowlen

        for k in maindict:
            for itemstamp in maindict[k]:
                if timestamp == itemstamp:
                    maindict[k].remove(itemstamp)
        if currbit == "1":
            maindict[1].append(timestamp)
            for buckett in bucketlist:
                if len(maindict[buckett]) > 2:
                    maindict[buckett].pop(0)
                    temp = maindict[buckett].pop(0)
                    if buckett != bucketlist[-1]:
                        maindict[2*buckett].append(temp)
                else:
                    break

    estimatedcount = 0
    biggestbucket = 0
    one = 0
    for k in bucketlist:
        if len(maindict[k]) > 0:
            biggestbucket = maindict[k][0]
    for k in bucketlist:
        for tstamp in maindict[k]:
            if tstamp != biggestbucket:
                estimatedcount += k
            else:
                estimatedcount += 0.5 * k

    print "\nEstimated number of ones in the last 1000 bits: %d" % (estimatedcount)
    for bit in foractualcount:
        if bit == "1":
            one += 1
    print "Actual number of ones in the last 1000 bits: %d" % (one)
    # print "error %: ",abs((100)*((one-estimatedcount)/float(one)))

flag = 0

def arraywork(rdd):
    global flag
    global mainarray
    tempArray = rdd.collect()
    lentemparray = len(tempArray)
    lenmainarray = len(mainarray)

    if flag==0:
        if lenmainarray<windowlen:
            mainarray.extend(tempArray)
        else:
            flag = 1
            dgim(mainarray)

    if flag==1:
        dgim(tempArray)


words.foreachRDD(arraywork)
ssc.start()
ssc.awaitTermination()

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import breeze.numerics.abs
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

object DGIMAlgorithm {

  var flag = 0
  var mainArray  = Array[String]()
  val windowlen = 1000
  var mainDict = Map[Int,ListBuffer[Int]]()
  val totalbuckets = 1 + (math.log(windowlen) / math.log(2)).toInt
  //  println("total buckets", totalbuckets)
  var bucketlist = new ListBuffer[Int]()
  var timestamp = 0
  var actualCount = 0
  var foractualcount = ListBuffer[Int]()

  for(i<-0 to totalbuckets-1){
    var bucketcap = math.pow(2,i).toInt
    bucketlist.append(bucketcap)
    mainDict(bucketcap) = new ListBuffer[Int]
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("DGIM Algorithm")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(Line => Line.split(" "))
    words.foreachRDD(x => arraywork(x))
    ssc.start()
    ssc.awaitTermination()

  }

  def arraywork(value: RDD[String]): Unit ={
    //    println("arraywork")
    var tempArray = value.collect()
    var lentempArray = tempArray.length
    var lenmainArray = mainArray.length
    dgim(tempArray)

//    if(flag==0){
//      if(lenmainArray<windowlen){
//        mainArray ++= tempArray
//
//      }
//      else{
//        flag = 1
//        dgim(tempArray)
//      }
//    }
//    else{
//      dgim(tempArray)
//    }

  }

  def dgim(strings: Array[String]): Unit ={
    //    println("dgim")
    var lenarr = strings.length

    for(curr<-strings){
      foractualcount.append(curr.toInt)
      if(foractualcount.length >= windowlen){
        foractualcount.remove(0)
      }

      timestamp = (timestamp+1)%windowlen

      for(k <- mainDict) {
        for (itemstamp <- mainDict(k._1)) {
          if (timestamp == itemstamp) {
            mainDict(k._1)-=itemstamp
          }
        }
      }

      if(curr =="1"){
        mainDict(1).append(timestamp)
        for(buckett <- bucketlist){
          if(mainDict(buckett).length > 2){
            mainDict(buckett) -= mainDict(buckett).head
            val temp = mainDict(buckett).head
            mainDict(buckett) -= mainDict(buckett).head
            if(buckett != bucketlist.reverse.head){
              mainDict(2*buckett).append(temp)
            }
          }
        }
      }

    }

    var estimatedcount = 0
    var biggestbucket = 0
    var one = 0
    for(k<-bucketlist){
      if(mainDict(k).length>0){
        biggestbucket = mainDict(k)(0)
      }
    }
    for(k<-bucketlist){
      for(tstamp <- mainDict(k)){
        if(tstamp!=biggestbucket){
          estimatedcount += k
        }
        else{
          estimatedcount += (0.5*k).toInt
        }
      }
    }

    println("\nEstimated number of ones in the last 1000 bits:" + estimatedcount)
    for(bit<-foractualcount){
      //      println("bit",bit)
      if(bit == 1) {
        one += 1
      }

    }

    println("Actual number of ones in the last 1000 bits: " + one)
    var error = abs((100)*((one-estimatedcount)/one.toFloat))
    //    println("error %: ",error)
    //    if(error>50){
    //      println("************************************************************ do something wrong code ********************************")
    //    }


  }

}
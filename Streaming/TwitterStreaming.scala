import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

import scala.collection.mutable.{ListMap, Map}

//object Surbhi_Batra_TwitterStreaming {
//
//}


object TwitterStreaming {

  var tweetsSeen=0
  var sofarlength=0
  //    val hashTagList = Array.ofDim[String](rows, cols)
  //    var hashTaglist = new ArrayList[String[]]()
  //var btwDictionary = Map[(graphx.VertexId, graphx.VertexId), Float]().withDefaultValue(0)

  var hashtaglist = Map[Int,Array[String]]()
  var lengthlist = Map[Int,Int]().withDefaultValue(0)
  var mainhash = Map[String,Int]().withDefaultValue(0)


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.ERROR)
    }


    val consumerKey = "eAqOfvyNz6j7vW7ajvWEcs1FV"
    val consumerSecret = "3WML0dLBTMzkAtPp8pGubEUi1ANpUkF7KZozJKEO3DfHAEoVv7"
    val accessToken = "938090581783277568-yEEHI24wa5CI99oyq0JB46OUwS90GY2"
    val accessTokenSecret = "zfs2pW862uWKmQNIJMxF2cPgdf5zhsAmq91VNFv8lVs7i"
    val filters = List("#", "California")

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val stream = TwitterUtils.createStream(ssc, None, filters)


    stream.foreachRDD(s => hot(s))
    //    println("***********")




    ssc.start()
    ssc.awaitTermination()
  }
  def hot(value: RDD[Status]): Unit ={
    val tweet = value.collect()
    val tweetslength = value.map(x => x.getText.length).collect()

    //    tweetslength.foreach(println)
    //    println("tweets seen ",tweetsSeen)

    sofarlength += tweetslength.sum

    if(tweetsSeen<100){

      val tweet_hashtags =value.flatMap(x=>x.getHashtagEntities).map(x=>x.getText).collect()
      hashtaglist(tweetsSeen) = tweet_hashtags
      for (ht <- 0 to tweet_hashtags.length-1){
        //        mainhash(ht) += 1
        mainhash(tweet_hashtags(ht)) = 1 + mainhash.getOrElse(tweet_hashtags(ht),0)

      }
      //      hashtaglist.foreach(println)
      //      println("____")


    }
    else{
      val r = scala.util.Random
      val rn = r.nextInt(tweetsSeen)+1
      if(rn<=100){
        sofarlength -= lengthlist(rn)
        val tweet_hashtags =value.flatMap(x=>x.getHashtagEntities).map(x=>x.getText).collect()
        hashtaglist(tweetsSeen) = tweet_hashtags
        for (ht <- 0 to tweet_hashtags.length-1){
          mainhash(tweet_hashtags(ht)) = mainhash.getOrElse(tweet_hashtags(ht),0)+1
        }
        println("Number of twitter from the beginning: " + tweetsSeen)

        println("Top 5 hot hashtags:")
        val a = ListMap(mainhash.toSeq.sortWith(_._2 > _._2):_*).slice(0,5)
        //        a.foreach(k => println(k._1,":",k._2))
        a.foreach(k => println(k._1 + ":" + k._2))




        println("The average length of the twitter is: " + (sofarlength.toFloat / tweetsSeen.toFloat).toString)

        println("\n")


      }
    }
    tweetsSeen += tweetslength.length








  }

}


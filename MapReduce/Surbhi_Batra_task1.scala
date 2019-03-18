import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions._

object Task1 {
  def main(args: Array[String]): Unit = {
    val infile = args(0)
    val outfile = args(1)
    //    val conf = new SparkConf().setAppName("Task1").setMaster("local[2]")
    //    val sc = new SparkContext(conf)
    //    println(sc)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    import sqlContext.implicits._
    //    val file = sc.textFile(infile)
    //
    //    val df = file.map(line => line.split(",")).
    //      filter(lines => lines(0)!= "userId").
    //      map(row => (row(0), row(1), row(2), row(4))).
    //      toDF("userId", "movieId","rating","timestamp")

    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "True")
      .load(infile)
    //      .load("ml-latest-small/ratings.csv")

    val result = df.groupBy("movieId").agg(mean("rating").alias("rating_avg"))
    result.orderBy("movieId")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outfile)
    //      .save("scala_task1_small.csv")
  }
}
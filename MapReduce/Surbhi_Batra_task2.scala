import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions._

object Task2 {
  def main(args: Array[String]): Unit = {
    val in_ratings = args(0)
    val in_tags = args(1)
    val outfile = args(2)
    //    val conf = new SparkConf().setAppName("Task2").setMaster("local[2]")
    //    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df_ratings = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "True")
      .load(in_ratings)
    //      .load("ml-latest-small/ratings.csv")

    val df_tags = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "True")
      //      .load("ml-latest-small/tags.csv")
      .load(in_tags)

    val res = df_ratings.join(df_tags, usingColumn = "movieId")

    res.groupBy("tag").agg(mean("rating").alias("rating_avg"))
      .orderBy(desc("tag"))
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outfile)
    //      .save("scala_task2_small.csv")

  }
}
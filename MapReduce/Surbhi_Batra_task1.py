import pyspark as spark

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import DoubleType

import sys
infile = sys.argv[1]
outfile = sys.argv[2]

spark = SparkSession \
		.builder \
		.appName("Task1_try1") \
		.config("spark.some.connfig.option","some-value") \
		.getOrCreate()

# df = spark.read.csv("/home/surbhi/Documents/2018Summer/DM/Assignment_01/ml-latest-small/ratings.csv", header = True)
df = spark.read.csv(infile, header = True, inferSchema= True)
# df = df.withColumn("movieId", df["movieId"].cast(DoubleType()))
df = df.groupBy("movieId").agg(F.avg("rating").alias('rating_avg'))
df = df.orderBy("movieId")
df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(outfile)
df.show()

#$SPARK_HOME/bin/spark-submit solution/surbhi_batra_task1.py solution/ml-latest-small/ratings.csv outputFiles/surbhi_batra_result_task1_small.csv
#$SPARK_HOME/bin/spark-submit solution/surbhi_batra_task1.py solution/ml-20m/ratings.csv outputFiles/surbhi_batra_result_task1_big.csv

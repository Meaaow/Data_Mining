import pyspark as spark

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.functions import col, avg
from pyspark.sql.types import DoubleType

import sys

infile_ratings = sys.argv[1]
infile_tags = sys.argv[2]
outfile = sys.argv[3]

spark = SparkSession \
		.builder \
		.appName("Task2_try1") \
		.config("spark.some.connfig.option","some-value") \
		.getOrCreate()

# df = spark.read.csv("/home/surbhi/Documents/2018Summer/DM/Assignment_01/ml-latest-small/ratings.csv", header = True)
dfr = spark.read.csv(infile_ratings, header = True, inferSchema= True)
dft = spark.read.csv(infile_tags, header = True, inferSchema= True)
df = dft.join(dfr, dfr.movieId == dft.movieId, 'inner')
df = df.groupBy("tag").agg(F.avg("rating").alias('rating_avg'))
##df = df.select(dft.tag, dfr.rating).collect()
df = df.orderBy(df.tag.desc())
df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(outfile)

# $SPARK_HOME/bin/spark-submit solution/surbhi_batra_task2.py solution/ml-latest-small/ratings.csv solution/ml-latest-small/tags.csv outputFiles/surbhi_batra_result_task2_small.csv
# $SPARK_HOME/bin/spark-submit solution/surbhi_batra_task2.py solution/ml-20m/ratings.csv solution/ml-20m/tags.csv outputFiles/surbhi_batra_result_task2_big.csv
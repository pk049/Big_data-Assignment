# 3. Count number of movie ratings per year. Hint: convert time column to TIMESTAMP using from_unixtime().

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Q3")\
            .getOrCreate()

filepath = '/home/sunbeam/BigData/data/movies/ratings.csv'
rating_schema = ("userid int, movieid int, rating double, rtime Long")

ratings = spark.read\
            .option("header", "true")\
            .schema(rating_schema)\
            .csv(filepath)

ratings.show()

result = ratings.select(year(from_unixtime("rtime")).alias("year"))\
            .groupBy("year")\
            .count()\
            .orderBy(col("year").asc())

result.show()

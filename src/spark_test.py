from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MovieLensAnalytics")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

# Read MovieLens movies from HDFS
movies_path = "hdfs://namenode:9000/user/movielens/raw/movies.csv"

movies = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(movies_path)
)

print("Movies schema:")
movies.printSchema()

print("Total movies:", movies.count())

movies.show(5)

spark.stop()
"""
MovieLens Rating Analytics Platform
Stage 2: PySpark Distributed Data Cleaning & Analytics Pipeline

This script runs inside the Spark Docker container connected to the
Hadoop and MongoDB network. It:
1. Ingests raw MovieLens 25M data from HDFS (hdfs://namenode:9000/user/movielens/raw/)
2. Preprocesses and cleans movies and ratings
3. Computes 5 required analytical workloads using distributed Spark transformations
4. Persists analytical views and cleaned catalogs into MongoDB (mongodb://mongodb:27017/)
5. Exports compact summaries to /app/results/ for local visualization and reporting
"""

import os
import sys
import time
from pymongo import MongoClient, ASCENDING, DESCENDING
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    explode,
    regexp_extract,
    regexp_replace,
    trim,
    from_unixtime,
    year,
    count,
    avg,
    round as spark_round,
    desc,
    asc,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    LongType
)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
HDFS_BASE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_RAW_DIR = f"{HDFS_BASE}/user/movielens/raw"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB", "movielens_analytics")
RESULTS_DIR = os.getenv("RESULTS_DIR", "/app/results")


def get_spark_session(app_name="MovieLensAnalytics"):
    """
    Initializes an optimized PySpark Session for local cluster execution
    within Docker memory boundaries (tuned shuffle partitions and driver memory).
    """
    print(f"[1/7] Initializing SparkSession ({app_name})...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    # Set Spark log level to WARN to keep output clean and readable
    spark.sparkContext.setLogLevel("WARN")
    print(f"      Spark Version: {spark.version}")
    return spark


def load_raw_data(spark):
    """
    Loads raw CSV data from HDFS using explicit schemas for maximum efficiency.
    Avoids expensive schema inference passes over 25 million records.
    """
    print(f"[2/7] Loading raw data from HDFS at {HDFS_RAW_DIR}...")

    movies_schema = StructType([
        StructField("movieId", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("genres", StringType(), nullable=True)
    ])

    ratings_schema = StructType([
        StructField("userId", IntegerType(), nullable=False),
        StructField("movieId", IntegerType(), nullable=False),
        StructField("rating", DoubleType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False)
    ])

    movies_raw = (
        spark.read
        .option("header", "true")
        .schema(movies_schema)
        .csv(f"{HDFS_RAW_DIR}/movies.csv")
    )

    ratings_raw = (
        spark.read
        .option("header", "true")
        .schema(ratings_schema)
        .csv(f"{HDFS_RAW_DIR}/ratings.csv")
    )

    return movies_raw, ratings_raw


def clean_data(movies_raw, ratings_raw):
    """
    Cleans and structures movies and ratings data:
    - Extracts 4-digit release year from movie title
    - Cleans movie title text
    - Transforms genres into arrays and exploded forms
    - Converts rating unix timestamp into datetime and integer rating year
    - Validates rating boundaries [0.5, 5.0]
    """
    print("[3/7] Preprocessing and cleaning datasets...")

    # 1. Clean movies
    # Title format is typically: "Toy Story (1995)" or "Ready Player One (2018)"
    cleaned_movies = (
        movies_raw
        .filter(col("movieId").isNotNull())
        .withColumn("year", regexp_extract(col("title"), r"\((\d{4})\)", 1))
        .withColumn("year", when(col("year") != "", col("year").cast(IntegerType())).otherwise(None))
        .withColumn("clean_title", trim(regexp_replace(col("title"), r"\s*\(\d{4}\)\s*$", "")))
        .withColumn("genre_array", split(col("genres"), r"\|"))
    )

    # 2. Clean ratings
    cleaned_ratings = (
        ratings_raw
        .filter(
            col("userId").isNotNull() &
            col("movieId").isNotNull() &
            col("rating").between(0.5, 5.0)
        )
        .withColumn("rating_timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
        .withColumn("rating_year", year(from_unixtime(col("timestamp"))))
    )

    return cleaned_movies, cleaned_ratings


def compute_analytics(cleaned_movies, cleaned_ratings):
    """
    Executes the 5 primary Big Data analytical workloads using Spark distributed transformations:
    1. Movie count per genre
    2. High-confidence top movies (> 10k ratings, avg > 4.2)
    3. Most-rated movies of all time
    4. Most-rated movies in year 2018
    5. Rating volume and average rating by genre and year
    """
    print("[4/7] Performing distributed analytics with Spark...")

    # Query 1: Movies per genre
    # Explode genre array and filter out missing/null genres
    exploded_genres = (
        cleaned_movies
        .withColumn("genre", explode(col("genre_array")))
        .filter((col("genre") != "(no genres listed)") & (col("genre") != ""))
    )

    movies_per_genre = (
        exploded_genres
        .groupBy("genre")
        .agg(count("movieId").alias("movie_count"))
        .orderBy(desc("movie_count"))
    )

    # Pre-aggregate movie ratings to avoid repeated full joins on 25M rows
    movie_ratings_summary = (
        cleaned_ratings
        .groupBy("movieId")
        .agg(
            count("rating").alias("rating_count"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        )
    )

    # Combined movie catalog with summary metrics
    movies_with_stats = (
        cleaned_movies
        .join(movie_ratings_summary, on="movieId", how="inner")
        .select(
            col("movieId"),
            col("title"),
            col("clean_title"),
            col("year"),
            col("genres"),
            col("rating_count"),
            col("avg_rating")
        )
    )

    # Query 2: Movies with > 10,000 ratings and avg rating > 4.2
    high_confidence_top_movies = (
        movies_with_stats
        .filter((col("rating_count") > 10000) & (col("avg_rating") > 4.2))
        .orderBy(desc("avg_rating"), desc("rating_count"))
    )

    # Query 3: Most-rated movies of all time (Top 50)
    most_rated_all_time = (
        movies_with_stats
        .orderBy(desc("rating_count"))
        .limit(50)
    )

    # Query 4: Most-rated movies in selected year (2018)
    ratings_2018 = (
        cleaned_ratings
        .filter(col("rating_year") == 2018)
        .groupBy("movieId")
        .agg(
            count("rating").alias("rating_count_2018"),
            spark_round(avg("rating"), 2).alias("avg_rating_2018")
        )
    )

    most_rated_2018 = (
        cleaned_movies
        .join(ratings_2018, on="movieId", how="inner")
        .select(
            col("movieId"),
            col("title"),
            col("year"),
            col("genres"),
            col("rating_count_2018").alias("rating_count"),
            col("avg_rating_2018").alias("avg_rating")
        )
        .orderBy(desc("rating_count"))
        .limit(50)
    )

    # Query 5: Rating volume and average rating by genre and year
    # Join ratings with exploded genres
    genre_yearly_trends = (
        cleaned_ratings
        .join(
            exploded_genres.select("movieId", "genre"),
            on="movieId",
            how="inner"
        )
        .groupBy("genre", "rating_year")
        .agg(
            count("rating").alias("rating_count"),
            spark_round(avg("rating"), 3).alias("avg_rating")
        )
        .orderBy(asc("genre"), asc("rating_year"))
    )

    return {
        "movies_per_genre": movies_per_genre,
        "high_confidence_top_movies": high_confidence_top_movies,
        "most_rated_all_time": most_rated_all_time,
        "most_rated_2018": most_rated_2018,
        "genre_yearly_trends": genre_yearly_trends,
        "movies_with_stats": movies_with_stats
    }


def persist_to_mongodb(analytics_results):
    """
    Saves the aggregated analytical datasets and enriched catalog into MongoDB.
    Creates necessary indexes to ensure high query performance for downstream consumers.
    """
    print(f"[5/7] Persisting analytics results to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB_NAME]

    collections_to_save = [
        ("genre_counts", analytics_results["movies_per_genre"], [("movie_count", DESCENDING)]),
        ("top_rated_high_confidence", analytics_results["high_confidence_top_movies"], [("avg_rating", DESCENDING), ("rating_count", DESCENDING)]),
        ("most_rated_all_time", analytics_results["most_rated_all_time"], [("rating_count", DESCENDING)]),
        ("most_rated_2018", analytics_results["most_rated_2018"], [("rating_count", DESCENDING)]),
        ("genre_yearly_trends", analytics_results["genre_yearly_trends"], [("genre", ASCENDING), ("rating_year", ASCENDING)]),
    ]

    for coll_name, df, index_specs in collections_to_save:
        records = [row.asDict() for row in df.collect()]
        coll = db[coll_name]
        coll.drop()  # Idempotent write
        if records:
            coll.insert_many(records)
            for idx_field, idx_order in index_specs:
                coll.create_index([(idx_field, idx_order)])
            print(f"      -> Stored {len(records)} records in MongoDB collection '{coll_name}'.")

    # Also save top 5,000 active movies in movies_clean for rapid search/catalog lookups
    clean_catalog = (
        analytics_results["movies_with_stats"]
        .orderBy(desc("rating_count"))
        .limit(5000)
    )
    catalog_records = [row.asDict() for row in clean_catalog.collect()]
    coll = db["movies_clean"]
    coll.drop()
    if catalog_records:
        coll.insert_many(catalog_records)
        coll.create_index([("movieId", ASCENDING)], unique=True)
        coll.create_index([("rating_count", DESCENDING)])
        coll.create_index([("avg_rating", DESCENDING)])
        print(f"      -> Stored {len(catalog_records)} top catalog movies in MongoDB collection 'movies_clean'.")

    client.close()


def export_results_locally(analytics_results):
    """
    Exports summary datasets as lightweight CSV to RESULTS_DIR using standard library csv.
    This allows local Mac scripts (e.g. Matplotlib/Seaborn visualizer) to run
    without loading large raw data into memory.
    """
    import csv

    print(f"[6/7] Exporting summary datasets to {RESULTS_DIR}...")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    exports = [
        ("genre_counts.csv", analytics_results["movies_per_genre"]),
        ("top_rated_high_confidence.csv", analytics_results["high_confidence_top_movies"]),
        ("most_rated_all_time.csv", analytics_results["most_rated_all_time"]),
        ("most_rated_2018.csv", analytics_results["most_rated_2018"]),
        ("genre_yearly_trends.csv", analytics_results["genre_yearly_trends"])
    ]

    for filename, df in exports:
        output_path = os.path.join(RESULTS_DIR, filename)
        rows = [row.asDict() for row in df.collect()]
        if rows:
            fieldnames = list(rows[0].keys())
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"      -> Saved {len(rows)} rows to {output_path}")


def display_sample_outputs(analytics_results):
    """
    Prints preview tables of results to terminal for immediate verification.
    """
    print("\n" + "=" * 80)
    print("                    ANALYTICS SUMMARY PREVIEW")
    print("=" * 80)

    print("\n--- 1. Movies Per Genre ---")
    analytics_results["movies_per_genre"].show(10, truncate=False)

    print("\n--- 2. High-Confidence Top Movies (> 10k ratings, avg > 4.2) ---")
    analytics_results["high_confidence_top_movies"].show(10, truncate=False)

    print("\n--- 3. Most-Rated Movies of All Time ---")
    analytics_results["most_rated_all_time"].show(10, truncate=False)

    print("\n--- 4. Most-Rated Movies in 2018 ---")
    analytics_results["most_rated_2018"].show(10, truncate=False)

    print("\n--- 5. Sample Genre Trends Over Time ---")
    analytics_results["genre_yearly_trends"].filter(col("genre").isin("Action", "Drama", "Sci-Fi", "Comedy")).show(12, truncate=False)

    print("=" * 80 + "\n")


def main():
    start_time = time.time()
    print("=" * 80)
    print(" Starting MovieLens 25M Spark & MongoDB Analytics Pipeline")
    print("=" * 80)

    spark = get_spark_session()
    try:
        movies_raw, ratings_raw = load_raw_data(spark)
        cleaned_movies, cleaned_ratings = clean_data(movies_raw, ratings_raw)

        # Cache cleaned_movies as it is joined across multiple analytical queries
        cleaned_movies.cache()

        analytics_results = compute_analytics(cleaned_movies, cleaned_ratings)

        # Persist to MongoDB & Local Files
        persist_to_mongodb(analytics_results)
        export_results_locally(analytics_results)

        # Print console preview
        display_sample_outputs(analytics_results)

        elapsed = time.time() - start_time
        print(f"[7/7] Pipeline completed successfully in {elapsed:.2f} seconds!")

    except Exception as e:
        print(f"\n[ERROR] Pipeline execution failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()

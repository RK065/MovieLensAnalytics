# Movie Rating Analytics Platform using MongoDB, HDFS, and Apache Spark
**Big Data Engineering Project Report**

---

## 1. Executive Summary
This project designs and implements an end-to-end distributed Big Data analytics platform using **Apache Hadoop HDFS**, **Apache Spark (PySpark)**, and **MongoDB**. The platform ingests, cleans, analyzes, and visualizes the benchmark **MovieLens 25M** dataset (~25 million ratings across ~62,000 movies).

By combining distributed storage (HDFS), distributed memory computing (Spark), and high-performance document database indexing and aggregation (MongoDB), the system addresses traditional single-node computational bottlenecks. The system processes the entire 25-million-row rating matrix in ~64 seconds without overwhelming host machine memory.

---

## 2. Dataset Overview
- **Dataset**: MovieLens 25M (GroupLens Research, University of Minnesota)
- **Size**: ~720 MB raw CSV data
- **Records**:
  - `ratings.csv`: 25,000,095 ratings (userId, movieId, rating, timestamp)
  - `movies.csv`: 62,423 titles (movieId, title, genres)
  - `tags.csv`: 1,093,360 tags applied by users
- **Time Coverage**: January 1995 to November 2019

---

## 3. System Architecture & Topology

```
+-----------------------------------------------------------------------------------------+
| Docker Network: movielensanalytics_default                                              |
|                                                                                         |
|  +--------------------+         +-----------------------+         +------------------+  |
|  | Hadoop HDFS        |         | Apache Spark 4.0.1    |         | MongoDB 7.0      |  |
|  | - NameNode: 9000   | ------> | - PySpark Local[*]    | ------> | - Port: 27017    |  |
|  | - DataNode: 9864   |  Stream | - 2GB Driver Memory   |  Batch  | - Persistent DB  |  |
|  | (/user/movielens)  |   25M   | - 16 Partitions       |  Insert | (movielens_      |  |
|  +--------------------+         +-----------------------+         |  analytics)      |  |
+---------------------------------------------|---------------------+--------|------------+
                                              |                              |
                                              | Aggregated CSVs              | Client Read
                                              v                              v
                          +-------------------------------------------------------+
                          | macOS Host (Local Python Virtual Environment)         |
                          | - scripts/mongo_queries.py (Aggregation Pipelines)    |
                          | - scripts/visualize.py (Matplotlib & Seaborn)         |
                          | - reports/figures/ (Generated Visual Dashboard)       |
                          +-------------------------------------------------------+
```

### Architectural Decisions
1. **Containerized Cluster**: Hadoop NameNode, DataNode, and MongoDB run within an isolated Docker network (`movielensanalytics_default`). This ensures Spark can resolve DataNode hostnames directly during distributed block transfers.
2. **Schema Enforcement**: Explicit Spark schemas (`StructType`) are applied upon HDFS reading, avoiding costly double-passes over the 25M ratings file.
3. **Selective Aggregation & Memory Guard**: Large datasets are processed and aggregated strictly within the Spark engine. Only lightweight summary tables (13 to 5,000 documents) are materialized to MongoDB and exported locally, preventing local out-of-memory errors.

---

## 4. PySpark Data Processing & Analytical Workloads

### 4.1 Data Cleaning & Feature Engineering
- **Release Year Extraction**: Extracted release year using regex `r"\((\d{4})\)"` on movie titles; cast to integer.
- **Title Sanitization**: Stripped trailing year substrings to produce clean canonical movie titles.
- **Genre Normalization**: Split pipe-delimited strings (`Action|Adventure|Sci-Fi`) into arrays and exploded for individual genre-level metrics while filtering out `(no genres listed)`.
- **Timestamp Transformation**: Converted Unix timestamps to timestamp datatypes and isolated `rating_year`.

---

### 4.2 Analytical Workloads & Findings

#### Workload 1: Movies per Genre
- **Analysis**: Exploded genre arrays and counted distinct movies per genre.
- **Key Finding**: **Drama** is the most prevalent genre (25,606 titles), followed by **Comedy** (16,870 titles) and **Thriller** (8,654 titles). Niche categories like **Film-Noir** (353) and **IMAX** (195) represent high-specialization segments.

| Genre | Movie Count |
| :--- | :--- |
| Drama | 25,606 |
| Comedy | 16,870 |
| Thriller | 8,654 |
| Romance | 7,719 |
| Action | 7,348 |
| Horror | 5,989 |
| Documentary | 5,605 |
| Crime | 5,319 |

#### Workload 2: High-Confidence Top Rated Movies (> 10,000 ratings & avg > 4.2)
- **Analysis**: Filtered movies with at least 10,000 reviews and an average rating exceeding 4.2 stars to eliminate small-sample bias.
- **Key Finding**: Only 13 movies meet this threshold. *The Shawshank Redemption (1994)* ranks highest with an average rating of **4.41** across **81,482** reviews, followed by *The Godfather (1972)* at **4.32** (52,498 reviews) and *The Usual Suspects (1995)* at **4.28** (55,366 reviews).

| Rank | Title | Year | Average Rating | Total Ratings |
| :---: | :--- | :---: | :---: | :---: |
| 1 | The Shawshank Redemption | 1994 | **4.41** | 81,482 |
| 2 | The Godfather | 1972 | **4.32** | 52,498 |
| 3 | The Usual Suspects | 1995 | **4.28** | 55,366 |
| 4 | The Godfather: Part II | 1974 | **4.26** | 34,188 |
| 5 | Schindler's List | 1993 | **4.25** | 60,411 |
| 6 | Seven Samurai | 1954 | **4.25** | 13,367 |
| 7 | Rear Window | 1954 | **4.24** | 20,162 |
| 8 | 12 Angry Men | 1957 | **4.24** | 16,569 |
| 9 | Fight Club | 1999 | **4.23** | 58,773 |
| 10 | One Flew Over the Cuckoo's Nest | 1975 | **4.22** | 36,058 |

#### Workload 3: Most-Rated Movies of All Time
- **Analysis**: Ranked titles by raw review volume regardless of average rating.
- **Key Finding**: *Forrest Gump (1994)* holds the title for the most-reviewed movie of all time (81,491 reviews, 4.05 avg), followed closely by *The Shawshank Redemption (1994)* (81,482 reviews) and *Pulp Fiction (1994)* (79,672 reviews, 4.18 avg). The mid-1990s represents the pinnacle era for MovieLens user rating activity.

#### Workload 4: Most-Rated Movies in 2018
- **Analysis**: Filtered reviews timestamped in calendar year 2018.
- **Key Finding**: In 2018, *Inception (2010)* emerged as the most-reviewed title (4,283 reviews, 4.16 avg), overtaking 1990s classics like *The Matrix* (4,144 reviews) and *The Shawshank Redemption* (4,082 reviews).

#### Workload 5: Genre Trends Over Time (1996 - 2019)
- **Analysis**: Grouped ratings by review year and exploded genres to compute annual volume and average ratings.
- **Key Finding**: Across the 24-year tracking period, **Drama** accumulated over 10.96 million ratings (3.68 all-time average), while **Comedy** gathered 8.92 million ratings (3.41 average). High-prestige genres like **War** (3.80 average) and **Mystery** (3.73 average) consistently sustain higher ratings than **Horror** (3.30 average).

---

## 5. MongoDB Aggregations & Query Optimization

MongoDB stores the aggregated results across 6 collections:
1. `genre_counts`: Total movies per genre.
2. `top_rated_high_confidence`: Elite movies meeting sample size and score thresholds.
3. `most_rated_all_time`: Top 50 popular films of all time.
4. `most_rated_2018`: Top 50 popular films in 2018.
5. `genre_yearly_trends`: 460 records tracking yearly genre volume and scores.
6. `movies_clean`: 5,000 top catalog titles with pre-computed statistics.

### Advanced Aggregation Pipelines

#### Decade-by-Decade Production & Rating Quality
Calculated using a mathematical decade grouping pipeline:
```javascript
{
  $project: {
    decade: { $concat: [{ $toString: { $subtract: ["$year", { $mod: ["$year", 10] }] } }, "s"] },
    rating_count: "$rating_count",
    avg_rating: "$avg_rating"
  }
}
```
**Results**:
- **1920s - 1950s**: Older classic catalog films maintain higher average ratings (3.81 - 3.89) due to selection bias (only the finest vintage films are preserved and rated).
- **1990s**: The most heavily rated decade with **8,870,319** total ratings across 1,395 catalog movies (average rating: 3.24).
- **2000s**: 6,376,347 ratings across 1,478 catalog movies (average rating: 3.29).

---

## 6. Visualizations & Figures

All figures are automatically generated by `scripts/visualize.py` and saved in `reports/figures/`:
1. **`genre_distribution.png`**: Bar chart showing total movie inventory across 19 genres.
2. **`top_rated_high_confidence.png`**: Ranked horizontal bar plot showing the 13 movies with >10,000 reviews and >4.2 rating with star scores and rating badges.
3. **`most_rated_comparison.png`**: Dual-panel horizontal bar chart contrasting all-time most reviewed titles against the top 10 titles reviewed in 2018.
4. **`genre_yearly_rating_trends.png`**: Two-tier longitudinal chart illustrating annual rating scores and review volume from 1996 to 2019 across core genres.
5. **`genre_rating_volume_heatmap.png`**: Comprehensive heat map correlating user review intensity across genres and calendar years.

---

## 7. Execution Commands Summary

| Component | Target Environment | Command |
| :--- | :--- | :--- |
| **Start Infrastructure** | Host (Mac) | `docker compose up -d` |
| **Run Spark Pipeline** | Docker (`movielensanalytics_default`) | `docker run --rm --network movielensanalytics_default -v "$(pwd):/app" -w /app movielens-spark:4.0.1 /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g /app/src/spark_pipeline.py` |
| **Run MongoDB Aggregations** | Host (Mac Virtualenv) | `./venv/bin/python scripts/mongo_queries.py` |
| **Generate Visualizations** | Host (Mac Virtualenv) | `./venv/bin/python scripts/visualize.py` |
| **Master One-Click Run** | Host (Mac) | `./scripts/run_all.sh` |

---

## 8. Conclusion
The Movie Rating Analytics Platform demonstrates the successful synergy of Big Data technologies:
- **HDFS** guarantees fault-tolerant distributed storage for raw large-scale files.
- **Apache Spark** performs fast in-memory transformation and heavy matrix aggregations in ~64 seconds.
- **MongoDB** provides flexible document-based storage, multi-key indexing, and expressive aggregation pipelines for downstream consumption.
- **Matplotlib & Seaborn** deliver clear visual insights into cinema trends, user behavior, and historical rating patterns without memory strain.

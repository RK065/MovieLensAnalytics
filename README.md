# Movie Rating Analytics Platform using MongoDB, HDFS, and Apache Spark

An end-to-end Big Data engineering project analyzing the **MovieLens 25M** dataset (~25 million ratings across 62,000+ movies) using a distributed pipeline built with **Hadoop HDFS**, **Apache Spark 4.0.1 (PySpark)**, **MongoDB 7.0**, and **Python**.

---

## Architecture Overview

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

### Why Spark Runs Inside Docker
On macOS (especially Apple Silicon), Hadoop DataNodes communicate data block transfers using internal container hostnames and ports. Running the PySpark pipeline inside a container connected to `movielensanalytics_default` allows direct, low-latency communication with both `namenode:9000` and `datanode` block ports. Downstream querying and visualizations run directly on the Mac host against MongoDB port `27017` and exported summary files.

---

## Tech Stack
| Component | Technology | Role |
| :--- | :--- | :--- |
| **Distributed Storage** | Hadoop HDFS (3.2.1) | Reliable storage for raw CSV files (ratings, movies, tags) |
| **Distributed Compute** | Apache Spark (4.0.1) | In-memory distributed filtering, cleaning, and aggregations |
| **Document Database** | MongoDB (7.0) | High-speed document storage, indexing, and aggregation pipelines |
| **Visualization & Scripting** | Python 3.13, Pandas, Matplotlib, Seaborn | Local visual analytics and reporting |
| **Containerization** | Docker & Docker Compose | Cluster isolation and reproducible environments |

---

## Project Structure
```
MovieLensAnalytics/
├── docker-compose.yml          # Hadoop (NameNode, DataNode) & MongoDB services
├── Dockerfile                  # Spark container definition with PyMongo support
├── data/
│   └── raw/                    # Local raw data backup (movies, ratings, tags)
├── src/
│   ├── spark_pipeline.py       # Core PySpark distributed cleaning & analytics script
│   └── spark_test.py           # Verification script for HDFS connectivity
├── scripts/
│   ├── run_all.sh              # Master single-command orchestration script
│   ├── mongo_queries.py        # MongoDB indexes and aggregation pipelines
│   └── visualize.py            # Generates charts and saves to reports/figures/
├── results/                    # Aggregated CSV outputs from Spark
│   ├── genre_counts.csv
│   ├── top_rated_high_confidence.csv
│   ├── most_rated_all_time.csv
│   ├── most_rated_2018.csv
│   └── genre_yearly_trends.csv
├── reports/
│   ├── PROJECT_REPORT.md       # Comprehensive academic/technical report
│   └── figures/                # Publication-quality charts (PNG)
│       ├── genre_distribution.png
│       ├── top_rated_high_confidence.png
│       ├── most_rated_comparison.png
│       ├── genre_yearly_rating_trends.png
│       └── genre_rating_volume_heatmap.png
└── venv/                       # Local Python virtual environment
```

---

## Quick Start (One-Click Execution)

To run the entire pipeline end-to-end (starts services, runs Spark on 25M rows, executes MongoDB aggregations, and generates all charts):

```bash
./scripts/run_all.sh
```

---

## Step-by-Step Execution Guide

### 1. Start Cluster Infrastructure (Mac Host)
Ensure Docker Desktop is running, then start NameNode, DataNode, and MongoDB:
```bash
docker compose up -d
```
Verify all services are healthy:
```bash
docker compose ps
```
HDFS Web UI is accessible at: [http://localhost:9870](http://localhost:9870)

### 2. Verify HDFS Data (Mac Host)
Verify that the MovieLens 25M files exist in HDFS:
```bash
docker exec namenode hdfs dfs -ls /user/movielens/raw
```
Expected files:
- `/user/movielens/raw/movies.csv` (~3.0 MB)
- `/user/movielens/raw/ratings.csv` (~678.3 MB, 25M rows)
- `/user/movielens/raw/tags.csv` (~38.8 MB)

### 3. Build the Spark Runner Image (Mac Host)
Build the customized Spark 4.0.1 container with PyMongo installed:
```bash
docker build -t movielens-spark:4.0.1 .
```

### 4. Run PySpark Pipeline (Inside Docker)
Execute the distributed data cleaning and analytical workloads across all 25 million records:
```bash
docker run --rm \
  --network movielensanalytics_default \
  -v "$(pwd):/app" \
  -w /app \
  movielens-spark:4.0.1 \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --driver-memory 2g \
  /app/src/spark_pipeline.py
```
*Typical runtime: ~60 to 70 seconds on Apple Silicon.*

### 5. Run MongoDB Aggregation Pipelines (Mac Host)
Activate your local virtual environment and execute the MongoDB aggregation scripts:
```bash
./venv/bin/python scripts/mongo_queries.py
```
This script:
- Inspects MongoDB collections and verifies indexes.
- Runs a **Decade-by-Decade Analysis** pipeline (1920s to 2010s).
- Runs an **All-Time Genre Popularity** pipeline with total rating volumes.
- Runs an **Elite Multi-Genre Representation** pipeline.

### 6. Generate Visualizations (Mac Host)
Generate the visual analytics charts using Matplotlib and Seaborn:
```bash
./venv/bin/python scripts/visualize.py
```
Charts are output to [reports/figures/](file:///Users/rishikesh./MovieLensAnalytics/reports/figures/):
- `genre_distribution.png`: Bar chart of movies per genre.
- `top_rated_high_confidence.png`: Ranked horizontal bar chart of top movies (>10k ratings, avg > 4.2).
- `most_rated_comparison.png`: Most-rated of all time vs most-rated in 2018.
- `genre_yearly_rating_trends.png`: Rating scores and review volumes over time.
- `genre_rating_volume_heatmap.png`: Matrix heatmap of review activity by genre and year.

---

## Analytics Results & Key Findings

### 1. Movies Per Genre
- **Drama** (25,606) and **Comedy** (16,870) are the most common movie genres.
- **Thriller** (8,654), **Romance** (7,719), and **Action** (7,348) follow closely.

### 2. High-Confidence Top Rated Movies (>10,000 ratings, avg > 4.2)
Only **13 movies** qualify under this strict threshold, led by:
1. **The Shawshank Redemption (1994)** — Rating: **4.41** (81,482 reviews)
2. **The Godfather (1972)** — Rating: **4.32** (52,498 reviews)
3. **The Usual Suspects (1995)** — Rating: **4.28** (55,366 reviews)
4. **The Godfather: Part II (1974)** — Rating: **4.26** (34,188 reviews)
5. **Schindler's List (1993)** — Rating: **4.25** (60,411 reviews)

### 3. Most-Rated Movies of All Time
The top 3 most-reviewed titles across all 25 million records are all 1994 releases:
1. **Forrest Gump (1994)** — 81,491 ratings (avg: 4.05)
2. **The Shawshank Redemption (1994)** — 81,482 ratings (avg: 4.41)
3. **Pulp Fiction (1994)** — 79,672 ratings (avg: 4.18)

### 4. Most-Rated Movies in 2018
In 2018, modern blockbusters took center stage alongside enduring classics:
1. **Inception (2010)** — 4,283 ratings in 2018 (avg: 4.16)
2. **The Matrix (1999)** — 4,144 ratings in 2018 (avg: 4.14)
3. **The Shawshank Redemption (1994)** — 4,082 ratings in 2018 (avg: 4.33)

### 5. Multi-Year Trends & Decades
- **1990s** accounts for the highest rating concentration in the dataset with **8.87 million** ratings across 1,395 catalog movies.
- Classic films from the **1920s - 1950s** exhibit higher average ratings (~3.85) due to preservation and selection bias.

---

## Memory & Performance Management
- **Explicit Schemas**: Using `StructType` in Spark prevents multiple full file scans of the 678 MB CSV.
- **Tuned Shuffle Partitions**: Configured `spark.sql.shuffle.partitions=16` to match local multi-core CPUs without overhead.
- **Zero Local Memory Strain**: PySpark handles all distributed joins and aggregations. Only small aggregated result sets (13 to 5,000 records) are transferred to MongoDB and local scripts, guaranteeing that Mac host RAM is never exhausted.

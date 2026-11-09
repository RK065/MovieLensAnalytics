#!/usr/bin/env bash
# ==============================================================================
# Movie Rating Analytics Platform using MongoDB, HDFS, and Apache Spark
# Master Orchestration Script
# ==============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "================================================================================"
echo " Starting MovieLens Analytics Pipeline"
echo " Working Directory: $PROJECT_ROOT"
echo "================================================================================"

# 1. Verify Docker Containers are Running
echo "[Step 1/4] Checking Docker containers (namenode, datanode, mongodb)..."
docker compose up -d namenode datanode mongodb

# Ensure datanode is ready
echo "Waiting for HDFS and MongoDB to be healthy..."
docker compose ps

# 2. Run Spark Pipeline inside Docker
echo ""
echo "[Step 2/4] Executing Distributed PySpark Analytics on 25M MovieLens dataset..."
docker run --rm \
  --network movielensanalytics_default \
  -v "$PROJECT_ROOT:/app" \
  -w /app \
  movielens-spark:4.0.1 \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --driver-memory 2g \
  /app/src/spark_pipeline.py

# 3. Execute MongoDB Aggregations
echo ""
echo "[Step 3/4] Running MongoDB indexes and analytical aggregation pipelines..."
./venv/bin/python scripts/mongo_queries.py

# 4. Generate Visual Analytics Dashboard
echo ""
echo "[Step 4/4] Generating visual charts and plots..."
./venv/bin/python scripts/visualize.py

echo ""
echo "================================================================================"
echo " Pipeline Finished Successfully!"
echo " Results Directory: $PROJECT_ROOT/results"
echo " Figures Directory: $PROJECT_ROOT/reports/figures"
echo "================================================================================"

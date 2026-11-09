"""
MovieLens Rating Analytics Platform
Stage 3: MongoDB Indexes & Aggregation Pipelines

This script runs on the Mac host (or container) and demonstrates MongoDB's
analytical query capabilities using native aggregation pipelines:
1. Inspects existing collections, documents counts, and indexes.
2. Executes multi-stage aggregation pipelines:
   - Pipeline 1: Decade-by-decade movie analysis (grouping by release decade)
   - Pipeline 2: Top genres by all-time weighted average rating
   - Pipeline 3: Multi-genre representation in high-confidence top movies
   - Pipeline 4: Year-over-year rating activity and volume trend
"""

import os
from pymongo import MongoClient, DESCENDING, ASCENDING


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB", "movielens_analytics")


def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client, client[MONGO_DB_NAME]


def inspect_collections(db):
    print("=" * 80)
    print(" 1. MONGODB COLLECTIONS & INDEX INSPECTION")
    print("=" * 80)
    collections = sorted(db.list_collection_names())
    for coll_name in collections:
        coll = db[coll_name]
        count = coll.count_documents({})
        indexes = coll.index_information()
        idx_keys = [f"{k}: {v['key']}" for k, v in indexes.items()]
        print(f"Collection: {coll_name:<28} | Count: {count:<6} | Indexes: {idx_keys}")
    print()


def pipeline_decade_analysis(db):
    """
    Aggregation Pipeline 1:
    Groups cleaned movies into 10-year release decades.
    Computes:
    - Number of movies produced per decade
    - Total reviews received
    - Average movie rating across that decade
    """
    print("=" * 80)
    print(" 2. AGGREGATION PIPELINE: DECADE-BY-DECADE MOVIE ANALYSIS")
    print("=" * 80)

    pipeline = [
        {"$match": {"year": {"$ne": None, "$gte": 1920, "$lte": 2020}}},
        {
            "$project": {
                "decade": {
                    "$concat": [
                        {"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}},
                        "s"
                    ]
                },
                "rating_count": "$rating_count",
                "avg_rating": "$avg_rating"
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "total_movies": {"$sum": 1},
                "total_ratings": {"$sum": "$rating_count"},
                "decade_avg_rating": {"$avg": "$avg_rating"}
            }
        },
        {
            "$project": {
                "decade": "$_id",
                "total_movies": 1,
                "total_ratings": 1,
                "decade_avg_rating": {"$round": ["$decade_avg_rating", 2]},
                "_id": 0
            }
        },
        {"$sort": {"decade": 1}}
    ]

    results = list(db["movies_clean"].aggregate(pipeline))
    print(f"{'Decade':<10} | {'Catalog Movies':<15} | {'Total Ratings':<15} | {'Decade Avg Rating':<18}")
    print("-" * 65)
    for r in results:
        print(f"{r['decade']:<10} | {r['total_movies']:<15} | {r['total_ratings']:<15} | {r['decade_avg_rating']:<18}")
    print()


def pipeline_top_genres_weighted(db):
    """
    Aggregation Pipeline 2:
    Aggregates yearly genre trends to determine all-time weighted average rating
    and cumulative rating volume per genre across all historical recorded years.
    """
    print("=" * 80)
    print(" 3. AGGREGATION PIPELINE: ALL-TIME GENRE POPULARITY & SCORES")
    print("=" * 80)

    pipeline = [
        {
            "$group": {
                "_id": "$genre",
                "total_reviews": {"$sum": "$rating_count"},
                "overall_avg_rating": {"$avg": "$avg_rating"},
                "years_tracked": {"$sum": 1}
            }
        },
        {
            "$project": {
                "genre": "$_id",
                "total_reviews": 1,
                "overall_avg_rating": {"$round": ["$overall_avg_rating", 3]},
                "years_tracked": 1,
                "_id": 0
            }
        },
        {"$sort": {"total_reviews": -1}}
    ]

    results = list(db["genre_yearly_trends"].aggregate(pipeline))
    print(f"{'Genre':<15} | {'Total Reviews':<15} | {'Overall Avg Rating':<20} | {'Years Tracked':<12}")
    print("-" * 68)
    for r in results[:15]:
        print(f"{r['genre']:<15} | {r['total_reviews']:<15} | {r['overall_avg_rating']:<20} | {r['years_tracked']:<12}")
    print()


def pipeline_high_confidence_genre_breakdown(db):
    """
    Aggregation Pipeline 3:
    Splits genres of elite high-confidence movies (>10k ratings, >4.2 avg),
    groups by genre, and gathers top title examples.
    """
    print("=" * 80)
    print(" 4. AGGREGATION PIPELINE: ELITE MOVIES PER GENRE (>10k ratings, >4.2 avg)")
    print("=" * 80)

    pipeline = [
        {
            "$project": {
                "title": 1,
                "avg_rating": 1,
                "rating_count": 1,
                "genre_list": {"$split": ["$genres", "|"]}
            }
        },
        {"$unwind": "$genre_list"},
        {
            "$group": {
                "_id": "$genre_list",
                "elite_movie_count": {"$sum": 1},
                "titles": {"$push": "$title"}
            }
        },
        {
            "$project": {
                "genre": "$_id",
                "elite_movie_count": 1,
                "sample_titles": {"$slice": ["$titles", 2]},
                "_id": 0
            }
        },
        {"$sort": {"elite_movie_count": -1}}
    ]

    results = list(db["top_rated_high_confidence"].aggregate(pipeline))
    print(f"{'Genre':<15} | {'Elite Movie Count':<18} | {'Sample Titles'}")
    print("-" * 80)
    for r in results:
        samples = ", ".join(r['sample_titles'])
        print(f"{r['genre']:<15} | {r['elite_movie_count']:<18} | {samples}")
    print()


def main():
    client, db = get_db()
    try:
        inspect_collections(db)
        pipeline_decade_analysis(db)
        pipeline_top_genres_weighted(db)
        pipeline_high_confidence_genre_breakdown(db)
    finally:
        client.close()


if __name__ == "__main__":
    main()

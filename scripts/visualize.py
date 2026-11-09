"""
MovieLens Rating Analytics Platform
Stage 4: Visualizations & Analytics Dashboard

This script runs on the Mac host using the local virtual environment (venv).
It queries aggregated analytics data directly from MongoDB (with automatic
fallback to CSV exports in results/) and generates publication-quality
visual charts saved into reports/figures/.

Memory-Safe: Operates strictly on pre-aggregated tables (tens to hundreds of rows),
guaranteeing that 25 million records are never loaded into Mac local memory.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

# ------------------------------------------------------------------------------
# Configuration & Theme Styling
# ------------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB", "movielens_analytics")
FIGURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reports", "figures")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")

os.makedirs(FIGURES_DIR, exist_ok=True)

# Set high-quality seaborn style
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.labelsize": 12,
    "axes.labelweight": "bold",
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "figure.titlesize": 16,
    "figure.dpi": 200
})


def load_dataset(collection_name, csv_filename):
    """
    Attempts to read pre-aggregated dataset from MongoDB;
    falls back cleanly to results/ CSV if MongoDB is unreachable.
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db = client[MONGO_DB_NAME]
        cursor = db[collection_name].find({}, {"_id": 0})
        docs = list(cursor)
        client.close()
        if docs:
            return pd.DataFrame(docs)
    except Exception as e:
        print(f"[Notice] Could not load from MongoDB ({e}), falling back to CSV.")

    csv_path = os.path.join(RESULTS_DIR, csv_filename)
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Neither MongoDB collection '{collection_name}' nor file '{csv_path}' was found.")


def plot_genre_distribution():
    """Figure 1: Number of Movies per Genre"""
    df = load_dataset("genre_counts", "genre_counts.csv")
    df = df.sort_values(by="movie_count", ascending=False)

    plt.figure(figsize=(12, 6))
    bars = sns.barplot(
        data=df,
        x="genre",
        y="movie_count",
        palette="viridis",
        hue="genre",
        legend=False
    )
    plt.title("Movie Distribution by Genre (MovieLens 25M)")
    plt.xlabel("Genre")
    plt.ylabel("Number of Movies")
    plt.xticks(rotation=45, ha="right")

    # Add numeric labels on top of bars
    for p in bars.patches:
        val = int(p.get_height())
        bars.annotate(
            f"{val:,}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="bottom",
            fontsize=8,
            rotation=0,
            xytext=(0, 3),
            textcoords="offset points"
        )

    plt.tight_layout()
    output_path = os.path.join(FIGURES_DIR, "genre_distribution.png")
    plt.savefig(output_path)
    plt.close()
    print(f"[✓] Saved {output_path}")


def plot_top_rated_high_confidence():
    """Figure 2: Elite Movies with >10,000 ratings and average rating > 4.2"""
    df = load_dataset("top_rated_high_confidence", "top_rated_high_confidence.csv")
    df = df.sort_values(by=["avg_rating", "rating_count"], ascending=[True, True])

    plt.figure(figsize=(12, 7))
    palette = sns.color_palette("mako_r", len(df))
    bars = plt.barh(df["title"], df["avg_rating"], color=palette, edgecolor="gray", alpha=0.9)

    plt.title("Highest-Rated Movies with High Confidence (>10,000 Ratings, Avg > 4.2)")
    plt.xlabel("Average Rating (Stars)")
    plt.xlim(4.0, 4.5)

    for bar, (_, row) in zip(bars, df.iterrows()):
        plt.text(
            bar.get_width() + 0.005,
            bar.get_y() + bar.get_height() / 2,
            f"{row['avg_rating']:.2f} ({int(row['rating_count']):,} ratings)",
            va="center",
            fontsize=9,
            weight="bold"
        )

    plt.tight_layout()
    output_path = os.path.join(FIGURES_DIR, "top_rated_high_confidence.png")
    plt.savefig(output_path)
    plt.close()
    print(f"[✓] Saved {output_path}")


def plot_most_rated_comparison():
    """Figure 3: Most-Rated Movies of All Time vs Most-Rated in 2018"""
    df_all = load_dataset("most_rated_all_time", "most_rated_all_time.csv").head(10)
    df_2018 = load_dataset("most_rated_2018", "most_rated_2018.csv").head(10)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), sharey=False)

    # All-time top 10
    sns.barplot(
        data=df_all.sort_values(by="rating_count", ascending=True),
        y="title",
        x="rating_count",
        ax=axes[0],
        palette="crest_r",
        hue="title",
        legend=False
    )
    axes[0].set_title("Top 10 Most-Rated Movies of All Time")
    axes[0].set_xlabel("Total Rating Count")
    axes[0].set_ylabel("")

    for p in axes[0].patches:
        val = int(p.get_width())
        axes[0].annotate(
            f"{val:,}",
            (p.get_width(), p.get_y() + p.get_height() / 2.0),
            ha="left",
            va="center",
            fontsize=9,
            xytext=(4, 0),
            textcoords="offset points"
        )
    axes[0].set_xlim(0, max(df_all["rating_count"]) * 1.15)

    # 2018 top 10
    sns.barplot(
        data=df_2018.sort_values(by="rating_count", ascending=True),
        y="title",
        x="rating_count",
        ax=axes[1],
        palette="flare_r",
        hue="title",
        legend=False
    )
    axes[1].set_title("Top 10 Most-Rated Movies in Year 2018")
    axes[1].set_xlabel("Ratings Count (in 2018)")
    axes[1].set_ylabel("")

    for p in axes[1].patches:
        val = int(p.get_width())
        axes[1].annotate(
            f"{val:,}",
            (p.get_width(), p.get_y() + p.get_height() / 2.0),
            ha="left",
            va="center",
            fontsize=9,
            xytext=(4, 0),
            textcoords="offset points"
        )
    axes[1].set_xlim(0, max(df_2018["rating_count"]) * 1.15)

    plt.tight_layout()
    output_path = os.path.join(FIGURES_DIR, "most_rated_comparison.png")
    plt.savefig(output_path)
    plt.close()
    print(f"[✓] Saved {output_path}")


def plot_genre_yearly_trends():
    """Figure 4: Rating Volume and Average Score Trends Over Time for Core Genres"""
    df = load_dataset("genre_yearly_trends", "genre_yearly_trends.csv")
    core_genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Thriller", "Horror"]
    df_core = df[df["genre"].isin(core_genres) & (df["rating_year"] >= 1996) & (df["rating_year"] <= 2019)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    # Top panel: Average Rating Trends
    sns.lineplot(
        data=df_core,
        x="rating_year",
        y="avg_rating",
        hue="genre",
        marker="o",
        linewidth=2.2,
        ax=axes[0]
    )
    axes[0].set_title("Annual Average Rating Trends by Genre (1996 - 2019)")
    axes[0].set_ylabel("Average Rating (Stars)")
    axes[0].legend(title="Genre", bbox_to_anchor=(1.02, 1), loc="upper left")

    # Bottom panel: Rating Volume Trends
    sns.lineplot(
        data=df_core,
        x="rating_year",
        y="rating_count",
        hue="genre",
        marker="s",
        linewidth=2.2,
        ax=axes[1],
        legend=False
    )
    axes[1].set_title("Annual Rating Volume by Genre (1996 - 2019)")
    axes[1].set_ylabel("Rating Count")
    axes[1].set_xlabel("Review Year")

    axes[1].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"{int(x/1000):,}k" if x >= 1000 else int(x)))

    plt.tight_layout()
    output_path = os.path.join(FIGURES_DIR, "genre_yearly_rating_trends.png")
    plt.savefig(output_path)
    plt.close()
    print(f"[✓] Saved {output_path}")


def plot_genre_volume_heatmap():
    """Figure 5: Heatmap of Rating Volume by Genre across Years"""
    df = load_dataset("genre_yearly_trends", "genre_yearly_trends.csv")
    df_filtered = df[(df["rating_year"] >= 1996) & (df["rating_year"] <= 2019)]

    pivot = df_filtered.pivot(index="genre", columns="rating_year", values="rating_count").fillna(0)

    # Sort genres by all-time volume
    genre_totals = pivot.sum(axis=1).sort_values(ascending=False)
    pivot = pivot.loc[genre_totals.index]

    plt.figure(figsize=(15, 8))
    sns.heatmap(
        pivot / 1000.0,
        cmap="YlGnBu",
        linewidths=0.5,
        cbar_kws={"label": "Rating Volume (in thousands)"}
    )
    plt.title("Heatmap: Movie Rating Activity Across Genres and Years (1996 - 2019)")
    plt.xlabel("Review Year")
    plt.ylabel("Genre")

    plt.tight_layout()
    output_path = os.path.join(FIGURES_DIR, "genre_rating_volume_heatmap.png")
    plt.savefig(output_path)
    plt.close()
    print(f"[✓] Saved {output_path}")


def main():
    print("=" * 80)
    print(" Generating Visual Analytics Dashboard Figures")
    print(f" Output Directory: {FIGURES_DIR}")
    print("=" * 80)

    plot_genre_distribution()
    plot_top_rated_high_confidence()
    plot_most_rated_comparison()
    plot_genre_yearly_trends()
    plot_genre_volume_heatmap()

    print("=" * 80)
    print(" All figures generated successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()

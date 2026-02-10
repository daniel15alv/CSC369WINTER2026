# Week5/Week5Analysis.py
import polars as pl
from sklearn.cluster import KMeans

FEATURES_PATH = "../user_features.parquet"
OUT_PATH = "user_clusters.parquet"   
K = 4  

def zscore_safe(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """
    Z-score standardization, but avoids NaN when std == 0 by outputting 0.0 for that column.
    """
    return df.with_columns([
        pl.when(pl.col(c).std() == 0)
          .then(0.0)
          .otherwise((pl.col(c) - pl.col(c).mean()) / pl.col(c).std())
          .alias(c)
        for c in cols
    ])


def main() -> None:
    features_final = pl.read_parquet(FEATURES_PATH)

    cols = [
        "total_events",
        "active_duration_sec",
        "median_window_ms",
        "fast_ratio_p01",
        "unique_pixels",
        "spatial_spread",
    ]


    df = features_final.select(["user_key"] + cols).drop_nulls()

    user_keys = df.select("user_key")
    features_ml = df.select(cols)


    log_cols = [
        "total_events",
        "active_duration_sec",
        "median_window_ms",
        "unique_pixels",
        "spatial_spread",
    ]
    features_log = features_ml.with_columns([
        pl.col(c).log1p().alias(c) for c in log_cols
    ])

    features_std = zscore_safe(features_log, cols)

    nulls = features_std.null_count()
    if any(nulls.row(0)):
        raise ValueError(f"Still have nulls after preprocessing: {nulls}")

    X = features_std.to_numpy()

    km = KMeans(n_clusters=K, n_init=10, random_state=42)
    labels = km.fit_predict(X)

    out = user_keys.with_columns(pl.Series("cluster", labels))
    out.write_parquet(OUT_PATH)
    print(f"Wrote: {OUT_PATH} (k={K}, rows={out.height})")


    print(out.group_by("cluster").agg(pl.len().alias("n")).sort("n", descending=True))

    clusters = pl.read_parquet("user_clusters.parquet")
    features = pl.read_parquet("../user_features.parquet")

    summary = (
        features
        .join(clusters, on="user_key")
        .group_by("cluster")
        .agg([
            pl.len().alias("n"),
            pl.col("total_events").mean().alias("mean_events"),
            pl.col("active_duration_sec").mean().alias("mean_duration"),
            pl.col("median_window_ms").mean().alias("mean_median_window"),
            pl.col("fast_ratio_p01").mean().alias("mean_fast_ratio"),
            pl.col("unique_pixels").mean().alias("mean_unique_pixels"),
            pl.col("spatial_spread").mean().alias("mean_spread"),
        ])
        .sort("n", descending=True)
    )

    print(summary)


if __name__ == "__main__":
    main()

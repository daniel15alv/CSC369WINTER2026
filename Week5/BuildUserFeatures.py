import polars as pl

# Load compact r/place events
events = pl.scan_parquet("../events_compact.parquet")

# Activity/Intensity features
# how many total placements did the user make and
# how long did they participate for during entire span
user_activity = (
    events
    .group_by("user_key")
    .agg([
        pl.len().alias("total_events"),
        (pl.col("t_ms").max() - pl.col("t_ms").min()).alias("active_duration_ms"),
    ])
    .with_columns(
        (pl.col("active_duration_ms") / 1000).alias("active_duration_sec")
    )
    .drop("active_duration_ms")
)

# "Skillset" features
# median inter-event time for every user and 
# proportion of user's windows faster than "fast" threshold
windows = (
    events
    .sort(["user_key", "t_ms"])
    .with_columns(
        (pl.col("t_ms") - pl.col("t_ms").shift(1).over("user_key"))
        .alias("window_time_ms")
    )
    .filter(pl.col("window_time_ms").is_not_null())
    .filter(pl.col("window_time_ms") >= 0)
)

# Predetermined "fast" threshold (1st percentile of placements)
FAST_THRESHOLD_MS = (
    windows
    .select(pl.col("window_time_ms").quantile(0.01))
    .collect()
    .item()
)

print(f"FAST_THRESHOLD_MS (p01) = {FAST_THRESHOLD_MS:.0f} ms")

user_skillset = (
    windows
    .group_by("user_key")
    .agg([
        pl.col("window_time_ms").median().alias("median_window_ms"),
        (pl.col("window_time_ms") <= FAST_THRESHOLD_MS)
        .mean()
        .alias("fast_ratio_p01"),
    ])
)

# Join Activity/Intensity + Skillset features
features = (
    user_activity
    .join(user_skillset, on="user_key", how="left")
)

# "Creatvity" features
# number of unique pixels placed by a user and
# spatial spread of their placements (bounding box area)
user_unique_pixels = (
    events
    .group_by("user_key")
    .agg(
        pl.struct(["x", "y"]).n_unique().alias("unique_pixels")
    )
)

user_spatial = (
    events
    .group_by("user_key")
    .agg([
        pl.col("x").min().alias("min_x"),
        pl.col("x").max().alias("max_x"),
        pl.col("y").min().alias("min_y"),
        pl.col("y").max().alias("max_y"),
    ])
    .with_columns(
        (
            (pl.col("max_x").cast(pl.Int64) - pl.col("min_x").cast(pl.Int64) + 1) *
            (pl.col("max_y").cast(pl.Int64) - pl.col("min_y").cast(pl.Int64) + 1)
        ).alias("spatial_spread")
    )
    .select(["user_key", "spatial_spread"])
)

# Final per-user feature table
features_final = (
    features
    .join(user_unique_pixels, on="user_key", how="left")
    .join(user_spatial, on="user_key", how="left")
    .collect()
)

# Write features to disk for Week 5 analysis
features_final.write_parquet("../user_features.parquet")
print(f"Wrote ../user_features.parquet with {features_final.height} users")

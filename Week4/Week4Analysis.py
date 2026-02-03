import polars as pl

# Bucket 1 helper: inter-event windows
def compute_inter_event_windows(events_path: str) -> pl.LazyFrame:
    events = pl.scan_parquet(events_path)

    windows = (
        events
        .sort(["user_key", "t_ms"])
        .with_columns(
            (pl.col("t_ms") - pl.col("t_ms").shift(1).over("user_key")).alias("delta_ms")
        )
        .filter(pl.col("delta_ms").is_not_null())
        .filter(pl.col("delta_ms") >= 0)  # safety: drop weird negatives
    )
    return windows


# Bucket 1: Bot-like behavior
def detect_bot_like_users(
    events_path: str,
    fast_ratio_threshold: float = 0.8,
    min_total_events: int = 50,
    percentile_for_fast: float = 0.01,
    output_csv: str = "suspected_bots.csv",
    print_top_n: int = 20,
) -> None:
    windows = compute_inter_event_windows(events_path)

    # define "fast" relative to dataset
    q = (
        windows
        .select([
            pl.col("delta_ms").quantile(0.01).alias("p01"),
            pl.col("delta_ms").quantile(0.05).alias("p05"),
            pl.col("delta_ms").quantile(0.10).alias("p10"),
            pl.col("delta_ms").median().alias("median"),
        ])
        .collect()
    )
    print("\n[Bucket 1] Inter-event timing quantiles (ms):")
    print(q)

    fast_threshold_ms = float(
        windows.select(pl.col("delta_ms").quantile(percentile_for_fast).alias("p")).collect()["p"][0]
    )
    print(
        f"\n[Bucket 1] Using FAST_THRESHOLD_MS = p{int(percentile_for_fast*100):02d} "
        f"= {fast_threshold_ms:.0f} ms ({fast_threshold_ms/1000:.2f} s)"
    )

    per_user_stats = (
        windows
        .group_by("user_key")
        .agg([
            pl.len().alias("total_events"),
            (pl.col("delta_ms") <= fast_threshold_ms).sum().alias("fast_events"),
            pl.col("delta_ms").median().alias("median_delta_ms"),
            pl.col("delta_ms").std().alias("std_delta_ms"),
        ])
        .with_columns(
            (pl.col("fast_events") / pl.col("total_events")).alias("fast_ratio")
        )
    )

    suspected_bots = (
        per_user_stats
        .filter(
            (pl.col("fast_ratio") > fast_ratio_threshold) &
            (pl.col("total_events") > min_total_events)
        )
        .sort(["fast_ratio", "total_events"], descending=[True, True])
        .collect()
    )

    print(f"\n[Bucket 1] Suspected bots found: {suspected_bots.height}")
    if suspected_bots.height > 0:
        print(
            suspected_bots.select(
                ["user_key", "total_events", "fast_events", "fast_ratio", "median_delta_ms", "std_delta_ms"]
            ).head(print_top_n)
        )
    else:
        print("[Bucket 1] No users met the bot-like thresholds in this run.")

    suspected_bots.write_csv(output_csv)
    print(f"[Bucket 1] Wrote CSV: {output_csv}")



# Bucket 2: Coordinated pixel placements

def detect_coordinated_bursts(
    events_path: str,
    time_granularity_sec: int = 1,
    percentile_threshold: float = 0.99,
    output_csv: str = "coordinated_windows.csv",
    print_top_n: int = 10,
) -> None:
    events = pl.scan_parquet(events_path)

    # Group into fixed-length time windows (seconds since dataset start)
    windows = (
        events
        .with_columns((pl.col("t_ms") // (time_granularity_sec * 1000)).alias("t_bucket"))
        .group_by("t_bucket")
        .agg(pl.col("user_key").n_unique().alias("users_in_window"))
    )

    stats = (
        windows
        .select([
            pl.col("users_in_window").quantile(0.95).alias("p95"),
            pl.col("users_in_window").quantile(0.99).alias("p99"),
            pl.col("users_in_window").max().alias("max_users"),
        ])
        .collect()
    )

    print("\n[Bucket 2] Window user-count stats:")
    print(stats)

    p_thresh = (
        windows
        .select(pl.col("users_in_window").quantile(percentile_threshold).alias("p"))
        .collect()
    )["p"][0]
    print(f"[Bucket 2] Using threshold = p{int(percentile_threshold*100):02d} = {p_thresh:.0f} users/window")

    coordinated = (
        windows
        .filter(pl.col("users_in_window") > p_thresh)
        .sort("users_in_window", descending=True)
        .collect()
    )

    print(f"\n[Bucket 2] Coordinated windows found: {coordinated.height}")
    if coordinated.height > 0:
        print(coordinated.head(print_top_n))
    else:
        print("[Bucket 2] No windows exceeded the burst threshold in this run.")

    # Convert bucket index back to seconds since start
    coordinated = (
        coordinated
        .with_columns((pl.col("t_bucket") * time_granularity_sec).alias("t_sec"))
        .select(["t_sec", "users_in_window"])
    )

    coordinated.write_csv(output_csv)
    print(f"[Bucket 2] Wrote CSV: {output_csv}")


def main():
    EVENTS_PATH = "../events_compact.parquet"

    # Run Bucket 1
    BOT_FAST_RATIO = 0.8
    BOT_MIN_EVENTS = 50
    BOT_FAST_PERCENTILE = 0.01
    BOTS_OUT = "suspected_bots.csv"

    BURST_WINDOW_SEC = 1
    BURST_PERCENTILE = 0.99
    BURSTS_OUT = "coordinated_windows.csv"

    # Run bucket 2
    detect_bot_like_users(
        events_path=EVENTS_PATH,
        fast_ratio_threshold=BOT_FAST_RATIO,
        min_total_events=BOT_MIN_EVENTS,
        percentile_for_fast=BOT_FAST_PERCENTILE,
        output_csv=BOTS_OUT,
    )

    detect_coordinated_bursts(
        events_path=EVENTS_PATH,
        time_granularity_sec=BURST_WINDOW_SEC,
        percentile_threshold=BURST_PERCENTILE,
        output_csv=BURSTS_OUT,
    )


if __name__ == "__main__":
    main()

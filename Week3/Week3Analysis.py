# Selected Timeframe: 2022-04-04 01 to 2022-04-04 07 
import sys
import time
import polars as pl

# using reddit, determined exact 32 colors
COLOR_ID_TO_NAME = {
    0: "Black",
    1: "White",
    2: "Red",
    3: "Blue",
    4: "Green",
    5: "Yellow",
    6: "Purple",
    7: "Orange",
    8: "Brown",
    9: "Pink",
    10: "Light Blue",
    11: "Dark Blue",
    12: "Light Green",
    13: "Dark Green",
    14: "Light Red",
    15: "Dark Red",
    16: "Cyan",
    17: "Magenta",
    18: "Gray",
    19: "Light Gray",
    20: "Dark Gray",
    21: "Maroon",
    22: "Navy",
    23: "Teal",
    24: "Olive",
    25: "Beige",
    26: "Lavender",
    27: "Peach",
    28: "Mint",
    29: "Gold",
    30: "Silver",
    31: "Coral",
}


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 Week3Analysis.py <events_compact.parquet>")
        sys.exit(1)

    path = sys.argv[1]

    # Since t_ms_min = 0 corresponds to 2022-04-04 00:00 UTC, this is 01:00–07:00
    start_ts =  1 * 60 * 60 * 1000  # should be 3,600,000
    end_ts   = 7 * 60 * 60 * 1000   # should be 25,200,000

    t0 = time.perf_counter_ns()

    events = pl.scan_parquet(path)

    # Apply timeframe once and reuse it for all tasks
    filtered = events.filter(
        (pl.col("t_ms") >= start_ts) & (pl.col("t_ms") < end_ts)
    )
 
    # Task 1: Rank colors by distinct users

    task1_start = time.perf_counter_ns()

    colors_ranked = (
        filtered
        .group_by("color_id")
        .agg(pl.col("user_key").n_unique().alias("distinct_users"))
        .sort("distinct_users", descending=True)
        .limit(10)
        .collect()
        .with_columns(
            pl.col("color_id")
              .map_elements(lambda c: COLOR_ID_TO_NAME.get(c, f"Unknown({c})"))
              .alias("color_name")
        )
        .select(["color_name", "distinct_users"])
    )

    task1_ms = (time.perf_counter_ns() - task1_start) / 1_000_000

    print("\nTask 1: Distinct users per color (Top 10)")
    for i, (color, users) in enumerate(colors_ranked.iter_rows(), start=1):
        print(f"{i}. {color}: {users} users")
    print(f"Task 1 Execution Time (ms): {task1_ms:.2f}")

    # Task 2: Calculate Average Session Length
    # Session = user's activity within 15 minutes of inactivity

    task2_start = time.perf_counter_ns()

    WINDOW_MS = 15 * 60 * 1000  # 900,000 ms

    sessions = (
        filtered
        .sort(["user_key", "t_ms"])
        .with_columns(pl.col("t_ms").shift(1).over("user_key").alias("prev_ts"))
        .with_columns(
            (
                pl.col("prev_ts").is_null()
                | ((pl.col("t_ms") - pl.col("prev_ts")) >= WINDOW_MS)
            ).cast(pl.Int32).alias("is_new_session")
        )
        .with_columns(pl.col("is_new_session").cum_sum().over("user_key").alias("session_id"))
        .group_by(["user_key", "session_id"])
        .agg(
            pl.col("t_ms").min().alias("session_start"),
            pl.col("t_ms").max().alias("session_end"),
            pl.len().alias("events_in_session"),
        )
        .with_columns((pl.col("session_end") - pl.col("session_start")).alias("session_length_ms"))
    )

    avg_session = (
        sessions
        .filter(pl.col("events_in_session") > 1)
        .select(pl.col("session_length_ms").mean().alias("avg_session_length_ms"))
        .collect()
    )

    task2_ms = (time.perf_counter_ns() - task2_start) / 1_000_000
    avg_val = avg_session[0, "avg_session_length_ms"]

    # only include times where more than one pixel was placed 
    if avg_val is None:
        print("\nTask 2: Average Session Length: N/A (no sessions with more than one pixel placement in this timeframe)")
    else:
        print(f"\nTask 2: Average Session Length: {avg_val:.2f} ms")

    # Task 3: Pixel placement percentiles 
    # Calculate 50th, 75th, 90th, and 99th percentiles of pixels placed per user during timeframe

    task3_start = time.perf_counter_ns()

    counts = (
        filtered
        .group_by("user_key")
        .len()
        .select(pl.col("len").alias("pixel_count"))
        .collect()
    )

    p50 = counts["pixel_count"].quantile(0.50, interpolation="nearest")
    p75 = counts["pixel_count"].quantile(0.75, interpolation="nearest")
    p90 = counts["pixel_count"].quantile(0.90, interpolation="nearest")
    p99 = counts["pixel_count"].quantile(0.99, interpolation="nearest")

    task3_ms = (time.perf_counter_ns() - task3_start) / 1_000_000
    print("\nTask 3: Percentiles of Pixels Placed")
    print(f"50th percentile: {p50}")
    print(f"75th percentile: {p75}")
    print(f"90th percentile: {p90}")
    print(f"99th percentile: {p99}")
    print(f"Task 3 Execution Time (ms): {task3_ms:.2f}")

    total_ms = (time.perf_counter_ns() - t0) / 1_000_000
    print(f"\nTotal Execution Time (ms): {total_ms:.2f}")

# Task 4: First-time users in timeframe
# Count how many users placed their first pixel ever within specified timeframe
    task4_start = time.perf_counter_ns()

    first_time_users = (
        events
        .group_by("user_key")
        .agg(pl.col("t_ms").min().alias("first_t_ms"))
        .filter((pl.col("first_t_ms") >= start_ts) & (pl.col("first_t_ms") < end_ts))
        .select(pl.len().alias("first_time_users"))
        .collect()
    )["first_time_users"][0]

    task4_ms = (time.perf_counter_ns() - task4_start) / 1_000_000
    print(f"\nTask 4: First-Time Users: {first_time_users} users")
    print(f"Task 4 Execution Time (ms): {task4_ms:.2f}")


if __name__ == "__main__":
    main()

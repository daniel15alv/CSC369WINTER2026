import sys
import time
import polars as pl

def main():
    if len(sys.argv) != 6:
        print("Usage: python3 Week2PolarsAnalysis.py <file.parquet> <start_YYYY-MM-DD> <start_HH> <end_YYYY-MM-DD> <end_HH>")
        sys.exit(1)

    path, start_date, start_hh, end_date, end_hh = sys.argv[1:6]
    start_hour = f"{start_date} {start_hh}:00:00"
    end_hour   = f"{end_date} {end_hh}:00:00"

    t0 = time.perf_counter_ns()

    # Lazy scan = doesn't load everything into memory at once
    base = (
        pl.scan_parquet(path)
        .with_columns(
            pl.col("timestamp").str.slice(0, 19).alias("ts19")
        )
        .filter(
            (pl.col("ts19") >= start_hour) & (pl.col("ts19") < end_hour)
        )
        .select(["pixel_color", "coordinate"])
    )

    # Most placed color
    color_top = (
        base.group_by("pixel_color")
        .len()
        .sort("len", descending=True)
        .limit(1)
        .collect()
    )

    # Most placed coordinate
    pixel_top = (
        base.group_by("coordinate")
        .len()
        .sort("len", descending=True)
        .limit(1)
        .collect()
    )

    t1 = time.perf_counter_ns()
    ms = (t1 - t0) / 1_000_000

    if color_top.height == 0 or pixel_top.height == 0:
        print("No events found in the selected timeframe.")
        return

    most_color = color_top["pixel_color"][0]
    most_coord = pixel_top["coordinate"][0]

    # coordinate is "x,y"
    x, y = str(most_coord).split(",", 1)

    print(f"Execution Time (ms): {ms:.2f}")
    print(f"Most Placed Color: {most_color}")
    print(f"Most Placed Pixel Location: ({x}, {y})")

if __name__ == "__main__":
    main()

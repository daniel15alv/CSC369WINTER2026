import sys
import time
import pandas as pd

def main():
    if len(sys.argv) != 6:
        print("Usage: python3 Week2PandasAnalysis.py <file.parquet> <start_YYYY-MM-DD> <start_HH> <end_YYYY-MM-DD> <end_HH>")
        sys.exit(1)

    path, start_date, start_hh, end_date, end_hh = sys.argv[1:6]
    start_hour = f"{start_date} {start_hh}:00:00"
    end_hour   = f"{end_date} {end_hh}:00:00"

    t0 = time.perf_counter_ns()

    # Load parquet (requires pyarrow installed)
    df = pd.read_parquet(path, engine="pyarrow", columns=["timestamp", "pixel_color", "coordinate"])

    # Match prior logic: compare timestamp string prefix (YYYY-MM-DD HH:MM:SS)
    ts19 = df["timestamp"].astype(str).str.slice(0, 19)
    mask = (ts19 >= start_hour) & (ts19 < end_hour)
    sub = df.loc[mask, ["pixel_color", "coordinate"]]

    if sub.empty:
        print("No events found in the selected timeframe.")
        return

    # Most placed color
    most_color = sub["pixel_color"].value_counts().idxmax()

    # Most placed coordinate
    most_coord = sub["coordinate"].value_counts().idxmax()

    t1 = time.perf_counter_ns()
    ms = (t1 - t0) / 1_000_000

    x, y = str(most_coord).split(",", 1)

    print(f"Execution Time (ms): {ms:.2f}")
    print(f"Most Placed Color: {most_color}")
    print(f"Most Placed Pixel Location: ({x}, {y})")

if __name__ == "__main__":
    main()

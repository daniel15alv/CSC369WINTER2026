import sys
import polars as pl

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 FinalCompactEvents.py <events.parquet> <user_lookup.parquet> <output.parquet>")
        sys.exit(1)

    events_path, users_path, out_path = sys.argv[1:4]

    events = pl.scan_parquet(events_path)
    users  = pl.scan_parquet(users_path)

    # Base timestamp so we can store offsets in Int32
    t0 = events.select(pl.col("timestamp_ms").min()).collect().item()
    print(f"Timestamp base t0 (ms): {t0}")

    lf = (
        events
        .join(users, on="user_id", how="inner")
        .with_columns(
            (pl.col("timestamp_ms") - pl.lit(t0)).cast(pl.Int32).alias("t_ms"),
            pl.col("user_key").cast(pl.UInt32)
        )
        .select(["t_ms", "user_key", "color_id", "x", "y"])
    )

    lf.sink_parquet(out_path, compression="zstd", compression_level=10)
    print(f"Wrote compact events: {out_path}")

if __name__ == "__main__":
    main()

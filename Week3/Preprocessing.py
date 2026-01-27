import sys
import polars as pl

# this first helper only got file to 10 GB
def conversion_helper(inp: str) -> pl.LazyFrame:
    """
    Read gzip CSV and create compact columns except user_key:
      - timestamp_ms (Int64)
      - user_id (Utf8)
      - color_id (UInt8)
      - x, y (UInt16)
    """
    return (
        pl.scan_csv(
            inp,
            has_header=True,
            ignore_errors=True,
            schema_overrides={
                "timestamp": pl.Utf8,
                "user_id": pl.Utf8,
                "pixel_color": pl.Utf8,
                "coordinate": pl.Utf8,
            },
        )
        .with_columns(
            pl.col("timestamp")
              .str.slice(0, 19)
              .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
              .dt.epoch(time_unit="ms")
              .cast(pl.Int64)
              .alias("timestamp_ms")
        )
        .with_columns(
            pl.col("coordinate")
              .str.strip_chars('"')
              .str.split_exact(",", 1)
              .alias("xy")
        )
        .with_columns(
            pl.col("xy").struct.field("field_0").cast(pl.UInt16).alias("x"),
            pl.col("xy").struct.field("field_1").cast(pl.UInt16).alias("y"),
        )
        .with_columns(
            pl.col("pixel_color").cast(pl.Categorical).alias("pixel_color_cat")
        )
        .with_columns(
            pl.col("pixel_color_cat").to_physical().cast(pl.UInt8).alias("color_id")
        )
        .select(["timestamp_ms", "user_id", "color_id", "x", "y"])
    )

def main():
    if len(sys.argv) not in (3, 4):
        print("Usage: python3 Preprocessing.py <input.csv.gzip> <output_events.parquet> [output_user_lookup.parquet]")
        sys.exit(1)

    inp = sys.argv[1]
    out_events = sys.argv[2]
    out_lookup = sys.argv[3] if len(sys.argv) == 4 else None

    # Phase 1: Build user lookup (user_id -> user_key)
    base_for_users = conversion_helper(inp)

    # Unique + sort for stable IDs (sorting costs time, but makes IDs deterministic)
    user_lookup = (
        base_for_users
        .select("user_id")
        .unique()
        .sort("user_id")
        .with_row_index("user_key")
        .select(["user_key", "user_id"])
        .collect()
    )

    print(f"Built user lookup (users={user_lookup.height})")

    if out_lookup:
        user_lookup.write_parquet(out_lookup, compression="zstd", compression_level=10)
        print(f"Wrote user lookup: {out_lookup}")

    # Phase 2: Re-scan CSV and write final compact events with user_key
    base = conversion_helper(inp)

    # Compute t0 for offset-ms (keeps ms units but allows Int32 storage)
    t0 = base.select(pl.col("timestamp_ms").min().alias("t0")).collect()["t0"][0]
    print(f"Timestamp base t0 (ms): {t0}")

    users_lf = user_lookup.lazy()

    final_events = (
        base.join(users_lf, on="user_id", how="inner")
            .with_columns(
                (pl.col("timestamp_ms") - pl.lit(t0)).cast(pl.Int32).alias("t_ms"),
                pl.col("user_key").cast(pl.UInt32)
            )
            .select(["t_ms", "user_key", "color_id", "x", "y"])
    )

    final_events.sink_parquet(out_events, compression="zstd", compression_level=10)
    print(f"Wrote final compact events: {out_events}")

if __name__ == "__main__":
    main()

import sys
import polars as pl

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 Week3/BuildUserLookup.py <input_events.parquet> <output_user_lookup.parquet>")
        sys.exit(1)

    inp, out = sys.argv[1], sys.argv[2]

    # Unique users (sort for stable IDs across runs)
    users = (
        pl.scan_parquet(inp)
        .select("user_id")
        .unique()
        .sort("user_id")
        .with_row_index("user_key")  # 0..n-1
        .select(["user_key", "user_id"])
        .collect()
    )

    users.write_parquet(out, compression="zstd", compression_level=10)
    print(f"Wrote user lookup: {out} (users={users.height})")

if __name__ == "__main__":
    main()


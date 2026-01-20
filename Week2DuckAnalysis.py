import sys
import time
import duckdb

def main():
    path, start_date, start_hh, end_date, end_hh = sys.argv[1:6]
    start_hour = f"{start_date} {start_hh}:00:00"
    end_hour   = f"{end_date} {end_hh}:00:00"

    t0 = time.perf_counter_ns()
    con = duckdb.connect(database=":memory:")

    query = f"""
    WITH base AS (
        SELECT
            substr(timestamp, 1, 19) AS ts19,
            pixel_color,
            coordinate
        FROM parquet_scan('{path}')
    ),
    filtered AS (
        SELECT ts19, pixel_color, coordinate
        FROM base
        WHERE ts19 >= '{start_hour}'
          AND ts19 <  '{end_hour}'
    ),
    color_top AS (
        SELECT pixel_color, COUNT(*) AS c
        FROM filtered
        GROUP BY pixel_color
        ORDER BY c DESC
        LIMIT 1
    ),
    pixel_top AS (
        SELECT coordinate, COUNT(*) AS c
        FROM filtered
        GROUP BY coordinate
        ORDER BY c DESC
        LIMIT 1
    )
    SELECT
        (SELECT pixel_color FROM color_top) AS most_color,
        (SELECT coordinate  FROM pixel_top) AS most_coord;
    """

    row = con.execute(query).fetchone()
    most_color, most_coord = row if row is not None else (None, None)

    t1 = time.perf_counter_ns()
    ms = (t1 - t0) / 1_000_000

    if most_color is None or most_coord is None:
        print("No events found in the selected timeframe.")
        return

    x, y = most_coord.split(",", 1)
    print(f"Execution Time (ms): {ms:.2f}")
    print(f"Most Placed Color: {most_color}")
    print(f"Most Placed Pixel Location: ({x}, {y})")

if __name__ == "__main__":
    main()

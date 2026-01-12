import sys
import gzip
import csv
import time
from datetime import datetime, timezone

# variables in '2022_place_canvas_history': timestamp, user_id, pixel_color, coordinate
def parse_hour(date_str: str, hour_str: str) -> int:
    dt = datetime(
        int(date_str[0:4]), # year
        int(date_str[5:7]), # month
        int(date_str[8:10]), # day
        int(hour_str), # hour
        0, 0, 
        tzinfo=timezone.utc
    )
    return int(dt.timestamp())


# parse "timestamp"
def parse_ts(ts: str) -> int:
    # format of timestamps: YYYY-MM-DD HH:MM:SS.mmm UTC
    need = ts[0:19]  # ignore milliseconds and timezone
    return int(datetime(
        int(need[0:4]),
        int(need[5:7]),
        int(need[8:10]),
        int(need[11:13]),
        int(need[14:16]),
        int(need[17:19]),
        tzinfo=timezone.utc
    ).timestamp())
 

def main():

    path = sys.argv[1]
    start_date, start_hour = sys.argv[2], sys.argv[3]
    end_date, end_hour = sys.argv[4], sys.argv[5]

    start_epoch = parse_hour(start_date, start_hour)
    end_epoch = parse_hour(end_date, end_hour)
    if end_epoch <= start_epoch:
        print("Error: end time must be after start time.")
        sys.exit(1)

    color_counts = {}
    pixel_counts = {}

    color_get = color_counts.get
    pixel_get = pixel_counts.get

    t0 = time.perf_counter_ns()

    with gzip.open(path, "rt", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for ts, _user_id, color, coord in reader:
            ts_epoch = parse_ts(ts)
            if ts_epoch < start_epoch:
                continue
            if ts_epoch >= end_epoch:
                continue

            # Count color
            color_counts[color] = color_get(color, 0) + 1

            coord = coord.strip().strip('"')
            x_y = coord.split(",")
            if len(x_y) != 2:
                continue
            x_str, y_str = x_y
            key = (x_str, y_str)
            pixel_counts[key] = pixel_get(key, 0) + 1

    if not color_counts:
        print("No events found in the selected timeframe.")
        return


    # Find max color
    most_color = max(color_counts.items(), key=lambda x: x[1])[0]
    # Find max pixel
    most_pixel = max(pixel_counts.items(), key=lambda x: x[1])[0]

    t1 = time.perf_counter_ns()
    ms = (t1 - t0) / 1_000_000

    print(f"Execution Time (ms): {ms:.2f}")
    print(f"Most Placed Color: {most_color}")
    print(f"Most Placed Pixel Location: ({most_pixel[0]}, {most_pixel[1]})")

if __name__ == "__main__":
    main()
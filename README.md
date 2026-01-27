Week 3 Preprocessing Pipeline
Preprocessing was split into three different stages/files.
Each file produces an intermediate Parquet file that is to be used by the next (parquet files not inclued in submission).

1) python3 Preprocessing.py <input.csv.gzip> TEST_OUTPUT.parquet
    * reads initial CSV containing all pixel placement events
    * extracts every field (timestamp, user, color) as well as the x and y from coordinates
    * normalizes timestamp into ms, writes result to TEST_OUTPUT.parquet (still too big at around 11GB, so had to do more preprocessing)
2) python3 BuildUserLookup.py TEST_OUTPUT.parquet user_lookup.parquet
    * created compact mapping of unique user id to a user key
    * outputs a table with these mappings 
3) python3 FinalCompactEvents.py TEST_OUTPUT.parquet user_lookup.parquet events_compact.parquet
    * joins compact user keys into the event data
    * produces final events_compact.parquet (1.1 GB) with the 5 aforementioned fields

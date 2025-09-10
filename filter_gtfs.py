import os
import pandas as pd

BASE_DIR = "data/gtfs/london_operators"
OUT_DIR = os.path.join(BASE_DIR, "filtered/stop_times")
os.makedirs(OUT_DIR, exist_ok=True)

trips_f = pd.read_csv(os.path.join(BASE_DIR, "filtered/trips/trips.txt"))
target_trip_ids = set(trips_f["trip_id"])

input_file = os.path.join(BASE_DIR, "stop_times.txt")
output_file = os.path.join(OUT_DIR, "stop_times.txt")

chunksize = 100_000

# Write header first
header_df = pd.read_csv(input_file, nrows=0)
header_df.to_csv(output_file, index=False)

# Process in chunks
for chunk in pd.read_csv(input_file, chunksize=chunksize):
    filtered_chunk = chunk[chunk["trip_id"].isin(target_trip_ids)]
    if not filtered_chunk.empty:
        filtered_chunk.to_csv(output_file, mode='a', header=False, index=False)

print("✅ stop_times.txt filtering complete. File saved at:", output_file)

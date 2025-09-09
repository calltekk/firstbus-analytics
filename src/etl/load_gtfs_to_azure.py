import os
import pandas as pd
import pyodbc

# -----------------------------
# Paths
# -----------------------------
GTFS_DIR = "data/gtfs/london_operators"

# -----------------------------
# Azure SQL connection using environment variable
# -----------------------------
server = 'firstbusdata.database.windows.net'
database = 'firstbus_db'
username = 'adminuser'
password = os.environ.get("AZURE_SQL_PASSWORD")  # <-- get password from py environment
driver = '{ODBC Driver 18 for SQL Server}'

if not password:
    raise ValueError("AZURE_SQL_PASSWORD environment variable not set!")

conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
print("Connected to Azure SQL successfully!")

# -----------------------------
# Helper function for fast insert
# -----------------------------
def df_to_sql_fast(df, table_name):
    if df.empty:
        print(f"{table_name} is empty, skipping.")
        return

    # Drop table if exists
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    conn.commit()

    # Create table dynamically
    cols = ', '.join([f'[{col}] NVARCHAR(MAX)' for col in df.columns])
    cursor.execute(f"CREATE TABLE {table_name} ({cols})")
    conn.commit()

    # Fast insert
    cursor.fast_executemany = True
    placeholders = ', '.join(['?'] * len(df.columns))
    cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", df.values.tolist())
    conn.commit()
    print(f"{table_name} loaded successfully (fast insert)!")

# -----------------------------
# Step 1: Load agency.txt and filter
# -----------------------------
agency_df = pd.read_csv(os.path.join(GTFS_DIR, "agency.txt"))
target_agencies = ["London United", "London Sovereign", "London Transit"]
filtered_agency_df = agency_df[agency_df['agency_name'].isin(target_agencies)]
df_to_sql_fast(filtered_agency_df, "agency")

# -----------------------------
# Step 2: Load routes.txt filtered by agency_id
# -----------------------------
routes_df = pd.read_csv(os.path.join(GTFS_DIR, "routes.txt"))
filtered_routes_df = routes_df[routes_df['agency_id'].isin(filtered_agency_df['agency_id'])]
df_to_sql_fast(filtered_routes_df, "routes")

# -----------------------------
# Step 3: Load trips.txt filtered by route_id
# -----------------------------
trips_df = pd.read_csv(os.path.join(GTFS_DIR, "trips.txt"))
filtered_trips_df = trips_df[trips_df['route_id'].isin(filtered_routes_df['route_id'])]
df_to_sql_fast(filtered_trips_df, "trips")

# -----------------------------
# Step 4: Load stop_times.txt filtered by trip_id
# -----------------------------
stop_times_df = pd.read_csv(os.path.join(GTFS_DIR, "stop_times.txt"))
filtered_stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(filtered_trips_df['trip_id'])]
df_to_sql_fast(filtered_stop_times_df, "stop_times")

# -----------------------------
# Step 5: Load stops.txt filtered by stop_id
# -----------------------------
stops_df = pd.read_csv(os.path.join(GTFS_DIR, "stops.txt"))
filtered_stops_df = stops_df[stops_df['stop_id'].isin(filtered_stop_times_df['stop_id'])]
df_to_sql_fast(filtered_stops_df, "stops")

# -----------------------------
# Step 6: Load calendar.txt
# -----------------------------
calendar_df = pd.read_csv(os.path.join(GTFS_DIR, "calendar.txt"))
df_to_sql_fast(calendar_df, "calendar")

# -----------------------------
# Step 7: Load calendar_dates.txt if exists
# -----------------------------
calendar_dates_file = os.path.join(GTFS_DIR, "calendar_dates.txt")
if os.path.exists(calendar_dates_file):
    calendar_dates_df = pd.read_csv(calendar_dates_file)
    df_to_sql_fast(calendar_dates_df, "calendar_dates")

# -----------------------------
# Step 8: Optional GTFS files
# -----------------------------
optional_files = ["frequencies.txt", "shapes.txt", "feed_info.txt"]
for f in optional_files:
    file_path = os.path.join(GTFS_DIR, f)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df_to_sql_fast(df, f.replace(".txt", ""))

# -----------------------------
# Close connection
# -----------------------------
cursor.close()
conn.close()
print("All selected GTFS files loaded to Azure SQL successfully!")

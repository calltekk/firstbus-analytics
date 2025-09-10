import os
from dotenv import load_dotenv
import pandas as pd
import pyodbc

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()  # reads .env file

GTFS_DIR = os.getenv("GTFS_DIR", "data/gtfs/london_operators/filtered")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

if not AZURE_SQL_PASSWORD:
    raise ValueError("AZURE_SQL_PASSWORD environment variable not set!")

# -----------------------------
# Azure SQL connection
# -----------------------------
server = 'firstbusdata.database.windows.net'
database = 'firstbus_db'
username = 'adminuser'
driver = '{ODBC Driver 18 for SQL Server}'

conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={AZURE_SQL_PASSWORD};"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
print("Connected to Azure SQL successfully!")

# -----------------------------
# Helper function
# -----------------------------
def df_to_sql_fast(df, table_name):
    if df.empty:
        print(f"{table_name} is empty, skipping.")
        return

    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    conn.commit()

    cols = ', '.join([f'[{col}] NVARCHAR(MAX)' for col in df.columns])
    cursor.execute(f"CREATE TABLE {table_name} ({cols})")
    conn.commit()

    cursor.fast_executemany = True
    placeholders = ', '.join(['?'] * len(df.columns))
    cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", df.values.tolist())
    conn.commit()
    print(f"{table_name} loaded successfully (fast insert)!")

# -----------------------------
# Load filtered GTFS files
# -----------------------------
files_to_load = [
    "agency/agency.txt",
    "calendar/calendar.txt",
    "calendar_dates/calendar_dates.txt",
    "feed_info/feed_info.txt",
    "frequencies/frequencies.txt",
    "routes/routes.txt",
    "shapes/shapes.txt",
    "stop_times/stop_times.txt",
    "stops/stops.txt",
    "trips/trips.txt"
]

for f in files_to_load:
    file_path = os.path.join(GTFS_DIR, f)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df_to_sql_fast(df, f.replace(".txt", "").replace("/", "_"))

# -----------------------------
# Close connection
# -----------------------------
cursor.close()
conn.close()
print("All selected GTFS files loaded to Azure SQL successfully!")

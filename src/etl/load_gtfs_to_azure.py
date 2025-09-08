import pyodbc
import pandas as pd
import os

# -----------------------------
# Paths
# -----------------------------
GTFS_DIR = "data/gtfs"

# -----------------------------
# Azure SQL connection
# -----------------------------
server = 'firstbusdata.database.windows.net'
database = 'firstbus_db'
username = 'adminuser'
password = 'Metapha2004%'
driver = '{ODBC Driver 18 for SQL Server}'  # Make sure this driver is installed on your Mac

# Add TrustServerCertificate=yes to bypass SSL verification
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)

# Connect to Azure SQL
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
print("Connected to Azure SQL successfully!")

# -----------------------------
# Function to load CSV into SQL
# -----------------------------
def load_csv_to_sql(file_name, table_name):
    file_path = os.path.join(GTFS_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    df = pd.read_csv(file_path)

    # Drop table if it exists
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    conn.commit()

    # Create table dynamically
    cols = ', '.join([f'[{col}] NVARCHAR(MAX)' for col in df.columns])
    cursor.execute(f"CREATE TABLE {table_name} ({cols})")
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        placeholders = ', '.join(['?'] * len(row))
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row))
    conn.commit()
    print(f"{table_name} loaded successfully!")

# -----------------------------
# Load all GTFS files
# -----------------------------
gtfs_files = [
    ("stops.txt", "stops"),
    ("routes.txt", "routes"),
    ("trips.txt", "trips"),
    ("stop_times.txt", "stop_times"),
    ("calendar.txt", "calendar")
]

for file_name, table_name in gtfs_files:
    load_csv_to_sql(file_name, table_name)

# -----------------------------
# Close connection
# -----------------------------
cursor.close()
conn.close()
print("All GTFS files loaded to Azure SQL successfully!")

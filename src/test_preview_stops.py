import pyodbc

server = 'firstbusdata.database.windows.net'
database = 'firstbus_db'
username = 'adminuser'
password = 'Metapha2004%'
driver = '{ODBC Driver 18 for SQL Server}'

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

# Preview first 5 rows of stops
cursor.execute("SELECT TOP 5 * FROM stops")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()

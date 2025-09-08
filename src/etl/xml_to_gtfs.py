import os
import pandas as pd
from lxml import etree

# Paths
RAW_DIR = "data/raw"
GTFS_DIR = "data/gtfs"
XML_FILE = os.path.join(RAW_DIR, "firstbus_transxchange.xml")

# Make GTFS folder if it doesn't exist
os.makedirs(GTFS_DIR, exist_ok=True)

# Parse XML
tree = etree.parse(XML_FILE)
root = tree.getroot()

# -----------------------------
# 1️⃣ Extract Stops
# -----------------------------
stops = []
for stop_point in root.findall(".//StopPoint"):
    stop_id = stop_point.get("id")
    name = stop_point.findtext("CommonName")
    lat = stop_point.findtext("Latitude")
    lon = stop_point.findtext("Longitude")
    stops.append([stop_id, name, lat, lon])

stops_df = pd.DataFrame(stops, columns=["stop_id", "stop_name", "stop_lat", "stop_lon"])
stops_df.to_csv(os.path.join(GTFS_DIR, "stops.txt"), index=False)
print("stops.txt created")

# -----------------------------
# 2️⃣ Extract Routes
# -----------------------------
routes = []
for line in root.findall(".//Line"):
    route_id = line.get("id")
    route_name = line.findtext("LineName")
    routes.append([route_id, route_name, "BUS"])  # all First Bus are BUS

routes_df = pd.DataFrame(routes, columns=["route_id", "route_short_name", "route_type"])
routes_df.to_csv(os.path.join(GTFS_DIR, "routes.txt"), index=False)
print("routes.txt created")

# -----------------------------
# 3️⃣ Extract Trips and Stop Times
# -----------------------------
trips = []
stop_times = []

for journey_pattern in root.findall(".//JourneyPattern"):
    pattern_id = journey_pattern.get("id")
    line_ref = journey_pattern.findtext("LineRef")
    
    for journey in journey_pattern.findall(".//VehicleJourney"):
        trip_id = journey.get("id")
        service_id = "weekday"  # placeholder, can be refined later
        trips.append([trip_id, line_ref, service_id])
        
        stop_sequence = 1
        for stop_point_ref in journey.findall(".//JourneyPatternTimingLink/From"):
            stop_id = stop_point_ref.get("id")
            stop_times.append([trip_id, stop_id, stop_sequence])
            stop_sequence += 1

trips_df = pd.DataFrame(trips, columns=["trip_id", "route_id", "service_id"])
stop_times_df = pd.DataFrame(stop_times, columns=["trip_id", "stop_id", "stop_sequence"])

trips_df.to_csv(os.path.join(GTFS_DIR, "trips.txt"), index=False)
stop_times_df.to_csv(os.path.join(GTFS_DIR, "stop_times.txt"), index=False)
print("trips.txt and stop_times.txt created")

# -----------------------------
# 4️⃣ Calendar (placeholder)
# -----------------------------
calendar_df = pd.DataFrame([["weekday", 1,1,1,1,1,0,0, "20250101", "20251231"]],
                           columns=["service_id","monday","tuesday","wednesday","thursday",
                                    "friday","saturday","sunday","start_date","end_date"])
calendar_df.to_csv(os.path.join(GTFS_DIR, "calendar.txt"), index=False)
print("calendar.txt created")

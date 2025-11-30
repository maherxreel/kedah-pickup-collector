import os
import requests
import zipfile
import io
import pandas as pd
import duckdb
import http.server
import socketserver
import threading
import time

# --- CONFIG ---
DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# We use the public OSRM demo server (Rate limit: 1 request/sec)
OSRM_URL = "http://router.project-osrm.org/nearest/v1/driving/"

# --- PART 1: GOV DATA (Bus/Train) ---
def fetch_gov_data():
    print("--- [1/3] Starting Gov Data Fetch ---")
    sources = {
        "mybas_alor_setar": "https://api.data.gov.my/gtfs-static/mybas-alor-setar",
        "ktm_trains": "https://api.data.gov.my/gtfs-static/ktmb"
    }
    all_pickups = []
    
    for name, url in sources.items():
        try:
            r = requests.get(url)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    with z.open('stops.txt') as f:
                        df = pd.read_csv(f)
                        cols = ['stop_name', 'stop_lat', 'stop_lon']
                        df = df[cols].copy()
                        df['source'] = name
                        
                        # Filter KTM for Kedah logic (approx lat 5.0 to 6.5)
                        if name == "ktm_trains":
                            df = df[(df['stop_lat'] >= 5.0) & (df['stop_lat'] <= 6.5)]
                        
                        all_pickups.append(df)
        except Exception as e:
            print(f"Error fetching {name}: {e}")

    if all_pickups:
        final_df = pd.concat(all_pickups)
        final_df.to_csv(f"{DATA_DIR}/gov_pickups.csv", index=False)
        print("--- [1/3] Saved gov_pickups.csv ---")

# --- PART 2: OVERTURE MAPS (Raw Data) ---
def fetch_overture_data():
    print("--- [2/3] Starting Overture Data Fetch ---")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    
    # Using 2025 data + Bounding Box for memory safety
    query = f"""
    COPY (
        SELECT 
            names.primary AS name, 
            categories.primary AS category, 
            ST_Y(geometry) AS lat, 
            ST_X(geometry) AS lon
        FROM read_parquet('s3://overturemaps-us-west-2/release/2025-11-19.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        WHERE 
            bbox.xmin > 100.0 AND bbox.xmax < 101.5
            AND bbox.ymin > 5.0 AND bbox.ymax < 7.0
            AND (
                categories.primary LIKE '%hotel%' 
                OR categories.primary LIKE '%transportation%'
                OR categories.primary LIKE '%shopping%'
            )
    ) TO '{DATA_DIR}/overture_pickups.csv' (HEADER, DELIMITER ',');
    """
    try:
        con.execute(query)
        print("--- [2/3] Saved overture_pickups.csv ---")
        # Trigger the snapping immediately after download
        snap_data_to_road()
    except Exception as e:
        print(f"Overture Error: {e}")

# --- PART 3: SNAP TO ROAD (The "Grab" Logic) ---
def snap_data_to_road():
    print("--- [3/3] Starting Road Snapping (This is slow) ---")
    input_path = f"{DATA_DIR}/overture_pickups.csv"
    output_path = f"{DATA_DIR}/snapped_pickups.csv"

    if not os.path.exists(input_path):
        print("Skipping Snapping: Overture file not found.")
        return

    df = pd.read_csv(input_path)
    # LIMIT FOR TESTING: Only do first 100 points to verify it works quickly.
    # Remove .head(100) if you want to run the full dataset (takes hours).
    df = df.head(100) 
    
    snapped_lats = []
    snapped_lons = []
    road_names = []

    print(f"Snapping {len(df)} points...")

    for index, row in df.iterrows():
        lat, lon = row['lat'], row['lon']
        coords = f"{lon},{lat}"
        try:
            # Call OSRM API
            url = f"{OSRM_URL}{coords}?number=1&radius=100"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if data.get('code') == 'Ok' and data.get('waypoints'):
                pt = data['waypoints'][0]['location']
                r_name = data['waypoints'][0]['name']
                snapped_lats.append(pt[1]) # Lat
                snapped_lons.append(pt[0]) # Lon
                road_names.append(r_name)
            else:
                snapped_lats.append(lat)
                snapped_lons.append(lon)
                road_names.append("SNAP_FAILED")
        except:
            snapped_lats.append(lat)
            snapped_lons.append(lon)
            road_names.append("ERROR")
        
        # SLEEP to respect API limits
        time.sleep(1.1)

    df['snapped_lat'] = snapped_lats
    df['snapped_lon'] = snapped_lons
    df['road_name'] = road_names
    
    df.to_csv(output_path, index=False)
    print(f"--- [3/3] DONE! Saved {output_path} ---")

# --- PART 4: SERVER ---
def run_server():
    PORT = 8000
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DATA_DIR, **kwargs)
    
    print(f"Serving files at port {PORT}")
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()

if __name__ == "__main__":
    # Start processes in background
    t1 = threading.Thread(target=fetch_gov_data)
    t2 = threading.Thread(target=fetch_overture_data)
    t1.start()
    t2.start()
    
    # Run server forever
    run_server()

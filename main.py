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
OSRM_URL = "http://router.project-osrm.org/nearest/v1/driving/"

# --- PART 1: GOV DATA ---
def fetch_gov_data():
    print("--- [1/3] Starting Gov Data Fetch ---")
    try:
        # (Same Gov logic as before - this part is safe)
        url = "https://api.data.gov.my/gtfs-static/mybas-alor-setar"
        r = requests.get(url)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open('stops.txt') as f:
                    df = pd.read_csv(f)
                    df = df[['stop_name', 'stop_lat', 'stop_lon']].copy()
                    df['source'] = "mybas_gov"
                    df.to_csv(f"{DATA_DIR}/gov_pickups.csv", index=False)
                    print("--- [1/3] Saved gov_pickups.csv ---")
    except Exception as e:
        print(f"Gov Data Error: {e}")

# --- PART 2: OVERTURE MAPS (With Safety Check) ---
def fetch_overture_data():
    print("--- [2/3] Starting Overture Data Fetch ---")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    
    # We use a strict BBOX in SQL
    query = f"""
    COPY (
        SELECT 
            names.primary AS name, 
            categories.primary AS category, 
            ST_Y(geometry) AS lat, 
            ST_X(geometry) AS lon
        FROM read_parquet('s3://overturemaps-us-west-2/release/2025-11-19.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
       # UPDATED: Strict Alor Setar Box (No Thailand)
        WHERE 
            bbox.xmin > 100.30 AND bbox.xmax < 100.45
            AND bbox.ymin > 6.05 AND bbox.ymax < 6.20
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
        snap_data_to_road()
    except Exception as e:
        print(f"Overture Error: {e}")

# --- PART 3: ROBUST SNAPPING (The Fix) ---
def snap_data_to_road():
    print("--- [3/3] Starting Road Snapping ---")
    input_path = f"{DATA_DIR}/overture_pickups.csv"
    output_path = f"{DATA_DIR}/snapped_pickups.csv"

    if not os.path.exists(input_path):
        print("Error: Input file missing.")
        return

    df = pd.read_csv(input_path)
    
    # 1. SAFETY FILTER: Delete points that are not in Kedah
    # Kedah is approx Lat 5.0-7.0, Lon 100.0-101.5
    original_count = len(df)
    df = df[
        (df['lat'] >= 5.0) & (df['lat'] <= 7.0) & 
        (df['lon'] >= 100.0) & (df['lon'] <= 101.5)
    ].copy()
    print(f"Safety Check: Removed {original_count - len(df)} points that were in the wrong city.")
    
    # 2. Limit for testing (First 50 points only)
    # df = df.head(50) 

    print(f"Snapping {len(df)} points...")
    
    snapped_lats = []
    snapped_lons = []
    road_names = []

    for index, row in df.iterrows():
        lat, lon = row['lat'], row['lon']
        
        # OSRM requires LONGITUDE first: "{lon},{lat}"
        coords = f"{lon},{lat}"
        
        try:
            # increased radius to 1000m to catch remote hotels
            url = f"{OSRM_URL}{coords}?number=1&radius=1000"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data['code'] == 'Ok' and data.get('waypoints'):
                    pt = data['waypoints'][0]['location']
                    # OSRM returns [lon, lat]
                    snapped_lats.append(pt[1]) 
                    snapped_lons.append(pt[0])
                    road_names.append(data['waypoints'][0]['name'])
                else:
                    # Snapping failed (no road found) -> Keep original
                    snapped_lats.append(lat)
                    snapped_lons.append(lon)
                    road_names.append("NO_ROAD_FOUND")
            else:
                snapped_lats.append(lat)
                snapped_lons.append(lon)
                road_names.append(f"API_ERROR_{response.status_code}")
                
        except Exception as e:
            snapped_lats.append(lat)
            snapped_lons.append(lon)
            road_names.append("REQ_FAILED")
        
        # Sleep to avoid getting blocked
        time.sleep(1.0)

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
    t1 = threading.Thread(target=fetch_gov_data)
    t2 = threading.Thread(target=fetch_overture_data)
    t1.start()
    t2.start()
    run_server()

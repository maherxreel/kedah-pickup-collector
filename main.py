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
import json

# --- CONFIG ---
DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# --- PART 1: GOV DATA ---
def fetch_gov_data():
    print("--- [1/3] Starting Gov Data Fetch ---")
    try:
        if os.path.exists(f"{DATA_DIR}/gov_pickups.csv"):
            os.remove(f"{DATA_DIR}/gov_pickups.csv")
    except: pass

    try:
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

# --- PART 2: OVERTURE MAPS ---
def fetch_overture_data():
    print("--- [2/3] Starting Overture Data Fetch ---")
    output_file = f"{DATA_DIR}/overture_pickups.csv"
    if os.path.exists(output_file): os.remove(output_file)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    
    query = f"""
    COPY (
        SELECT 
            names.primary AS name, 
            categories.primary AS category, 
            ST_Y(geometry) AS lat, 
            ST_X(geometry) AS lon
        FROM read_parquet('s3://overturemaps-us-west-2/release/2025-11-19.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        WHERE 
            bbox.xmin > 100.30 AND bbox.xmax < 100.45
            AND bbox.ymin > 6.05 AND bbox.ymax < 6.20
            AND (
                categories.primary LIKE '%hotel%' 
                OR categories.primary LIKE '%transportation%'
                OR categories.primary LIKE '%shopping%'
            )
    ) TO '{output_file}' (HEADER, DELIMITER ',');
    """
    try:
        con.execute(query)
        print("--- [2/3] Saved overture_pickups.csv ---")
        snap_data_to_road()
    except Exception as e:
        print(f"Overture Error: {e}")

# --- PART 3: SNAP TO ROAD (VALHALLA EDITION) ---
def snap_data_to_road():
    print("--- [3/3] Starting Road Snapping (Valhalla Mode) ---")
    input_path = f"{DATA_DIR}/overture_pickups.csv"
    output_path = f"{DATA_DIR}/snapped_pickups.csv"

    if not os.path.exists(input_path): return

    df = pd.read_csv(input_path)
    df = df[
        (df['lat'] > 6.0) & (df['lat'] < 6.25) & 
        (df['lon'] > 100.2) & (df['lon'] < 100.5)
    ].copy()

    print(f"Snapping {len(df)} points using Valhalla...")
    
    snapped_lats = []
    snapped_lons = []
    road_names = []

    # Valhalla Public Demo Server
    valhalla_url = "https://valhalla1.openstreetmap.de/locate"

    for index, row in df.iterrows():
        lat, lon = row['lat'], row['lon']
        
        try:
            # Valhalla expects a JSON payload
            payload = {
                "locations": [{"lat": lat, "lon": lon}],
                "costing": "auto",
                "radius": 200  # meters
            }
            
            response = requests.post(valhalla_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # Check for edges (roads)
                if data and len(data) > 0 and 'edges' in data[0]:
                    # Valhalla returns the "edge" (road segment). 
                    # We take the closest point on that edge.
                    # Note: Valhalla's 'locate' gives us info about the road, but slightly harder to get exact point.
                    # Simplification: We will trust the first result matches.
                    
                    # Wait! Valhalla 'locate' is for map matching. 
                    # Let's use the standard result.
                    
                    edge = data[0]['edges'][0]
                    # Valhalla doesn't always return the exact snapped coordinate in 'locate' easily 
                    # without more math, but let's check 'correlated_lat' if available or just use the input
                    # Actually, Valhalla is tricky for simple snapping.
                    
                    # LET'S FALLBACK TO OSRM BUT WITH A PROXY TRICK (HTTPS)
                    # The issue with OSRM previously might have been HTTP vs HTTPS or strict blocking.
                    # We will try the HTTPS version of openstreetmap.de which is usually open.
                    pass
            
            # REVISION: Valhalla is too complex for a quick script.
            # LET'S USE THE "ROBUST" OSRM via HTTPS.
            # Many cloud servers block HTTP but allow HTTPS.
            
            headers = {'User-Agent': 'Mozilla/5.0'}
            # Note the 's' in https
            osrm_secure = f"https://routing.openstreetmap.de/routed-car/nearest/v1/driving/{lon},{lat}?number=1&radius=1000"
            
            r = requests.get(osrm_secure, headers=headers, timeout=10)
            if r.status_code == 200:
                d = r.json()
                if 'waypoints' in d:
                    pt = d['waypoints'][0]['location']
                    snapped_lats.append(pt[1])
                    snapped_lons.append(pt[0])
                    road_names.append(d['waypoints'][0].get('name', 'Unnamed Road'))
                else:
                    raise Exception("No Waypoints")
            else:
                # If HTTPS fails, we mark as API_ERROR_SECURE
                snapped_lats.append(lat)
                snapped_lons.append(lon)
                road_names.append(f"API_ERROR_{r.status_code}")

        except Exception as e:
            snapped_lats.append(lat)
            snapped_lons.append(lon)
            road_names.append("REQ_FAILED")
        
        time.sleep(2.0) # Slower rate limit

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
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()

if __name__ == "__main__":
    t1 = threading.Thread(target=fetch_gov_data)
    t2 = threading.Thread(target=fetch_overture_data)
    t1.start()
    t2.start()
    run_server()

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
import googlemaps 

# --- CONFIG ---
DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# YOUR KEY
GOOGLE_API_KEY = "AIzaSyD_UHEIfFxUSL23EjlQSZEEGt_Znd-1rGg"

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
    
    # Self-clean to force fresh start
    if os.path.exists(output_file): os.remove(output_file)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    
    # STRICT ALOR SETAR BOX
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

# --- PART 3: GOOGLE SNAP TO ROADS ---
def snap_data_to_road():
    print("--- [3/3] Starting Road Snapping (Google Mode) ---")
    
    input_path = f"{DATA_DIR}/overture_pickups.csv"
    output_path = f"{DATA_DIR}/snapped_pickups.csv"

    if not os.path.exists(input_path):
        print("Input file missing.")
        return

    df = pd.read_csv(input_path)
    
    # Filter strict Kedah box just in case
    df = df[
        (df['lat'] > 6.0) & (df['lat'] < 6.25) & 
        (df['lon'] > 100.2) & (df['lon'] < 100.5)
    ].copy()
    
    print(f"Snapping {len(df)} points using Google API...")

    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

    snapped_lats = []
    snapped_lons = []
    road_names = []

    # Process in batches of 100 (Google Limit)
    chunk_size = 100
    records = df.to_dict('records')
    
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        path_chunk = [(r['lat'], r['lon']) for r in chunk]
        
        try:
            # Call Google API
            snapped_points = gmaps.snap_to_roads(path_chunk, interpolate=False)
            
            # Create a lookup dictionary: {originalIndex: (lat, lon)}
            snap_map = {}
            for item in snapped_points:
                if 'originalIndex' in item and 'location' in item:
                    snap_map[item['originalIndex']] = (
                        item['location']['latitude'],
                        item['location']['longitude']
                    )
            
            # Map back to original order
            for j in range(len(chunk)):
                if j in snap_map:
                    snapped_lats.append(snap_map[j][0])
                    snapped_lons.append(snap_map[j][1])
                    road_names.append("Google_Verified")
                else:
                    # No road found near point
                    snapped_lats.append(chunk[j]['lat'])
                    snapped_lons.append(chunk[j]['lon'])
                    road_names.append("No_Road_Nearby")
                    
        except Exception as e:
            print(f"Google Batch Error: {e}")
            # If batch fails, preserve originals
            for r in chunk:
                snapped_lats.append(r['lat'])
                snapped_lons.append(r['lon'])
                road_names.append("Google_Error")
        
        time.sleep(0.5) # Gentle rate limit

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

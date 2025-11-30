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

# --- PART 1: GOV DATA (Bus/Train) ---
def fetch_gov_data():
    print("Starting Gov Data Fetch...")
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
                        # Basic cleanup
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
        print("Saved gov_pickups.csv")

# --- PART 2: OVERTURE MAPS (Malls/Hotels via DuckDB) ---
def fetch_overture_data():
    print("Starting Overture Data Fetch (This takes time)...")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")

    # Query Kedah Bounding Box
    query = f"""
    COPY (
        SELECT 
            names.primary AS name, 
            categories.primary AS category, 
            ST_Y(geometry) AS lat, 
            ST_X(geometry) AS lon
        FROM read_parquet('s3://overturemaps-us-west-2/release/2024-11-13.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        WHERE 
            bbox.xmin > 100.3 AND bbox.xmax < 101.1
            AND bbox.ymin > 5.0 AND bbox.ymax < 6.5
            AND (
                categories.primary LIKE '%hotel%' 
                OR categories.primary LIKE '%transportation%'
                OR categories.primary LIKE '%shopping%'
            )
    ) TO '{DATA_DIR}/overture_pickups.csv' (HEADER, DELIMITER ',');
    """
    try:
        con.execute(query)
        print("Saved overture_pickups.csv")
    except Exception as e:
        print(f"Overture Error: {e}")

# --- PART 3: WEB SERVER (To download files) ---
def run_server():
    PORT = 8000
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DATA_DIR, **kwargs)

    print(f"Serving files at port {PORT}")
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()

if __name__ == "__main__":
    # Run data collection in background
    t1 = threading.Thread(target=fetch_gov_data)
    t2 = threading.Thread(target=fetch_overture_data)
    t1.start()
    t2.start()

    # Run server forever
    run_server()

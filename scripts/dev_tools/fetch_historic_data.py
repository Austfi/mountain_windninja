import os
import requests
import datetime
import sys

# Import daily_run for HRRR fetching logic
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import daily_run

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "temp", "grib_cache")

def download_file(url, local_path):
    if os.path.exists(local_path):
        print(f"  [SKIP] Exists: {os.path.basename(local_path)}")
        return True
        
    print(f"  Downloading: {url}")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"  [OK] Saved.")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False

def fetch_nam_hour(date_obj):
    # Pattern: https://noaa-nam-pds.s3.amazonaws.com/nam.YYYYMMDD/nam.tCCz.conusnest.hiresfFF.tm00.grib2
    # NAM cycles: 00, 06, 12, 18
    
    hour = date_obj.hour
    cycle = (hour // 6) * 6
    offset = hour - cycle
    
    ymd = date_obj.strftime("%Y%m%d")
    
    filename = f"nam.t{cycle:02d}z.conusnest.hiresf{offset:02d}.tm00.grib2"
    url = f"https://noaa-nam-pds.s3.amazonaws.com/nam.{ymd}/{filename}"
    
    local_path = os.path.join(CACHE_DIR, f"nam_{ymd}_{hour:02d}z.grib2") # Save as specific hour locally
    return download_file(url, local_path), local_path

def run_fetch(date_str):
    # date_str YYYYMMDD
    print(f">>> Fetching Historic Data for {date_str} (HRRR + NAM)")
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    start_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    
    for h in range(24):
        current_dt = start_dt + datetime.timedelta(hours=h)
        print(f"\n--- Hour {current_dt.strftime('%H:00')} ---")
        
        # 1. HRRR (Using existing logic)
        # Note: daily_run.download_hrrr_single_hour handles GCS fallback
        hrrr_path = daily_run.download_hrrr_single_hour(current_dt, CACHE_DIR)
        
        # 2. NAM (AWS S3)
        success, nam_path = fetch_nam_hour(current_dt)
        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="YYYYMMDD")
    args = parser.parse_args()
    
    run_fetch(args.date)

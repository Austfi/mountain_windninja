import requests
import datetime
import pandas as pd
import re
from bs4 import BeautifulSoup
import sys

# Station Config
STATIONS = [
    {
        "code": "CAKWS",
        "name_url": "Keystone+SA+-+Wind+Study+%28VailResort%29+12304+ft",
        "output_file": "obs_CAKWS.csv"
    },
    {
        "code": "CAKWP",
        "name_url": "Keystone+SA+-+Wapiti+%28VailResort%29+11978+ft",
        "output_file": "obs_CAKWP.csv"
    }
]

def fetch_obs(station_conf, hours_back=48):
    """
    Fetches observation data from CAIC website for a specific station.
    Returns a pandas DataFrame.
    """
    
    # Calculate range needed (hours back)
    # The URL expects a date and a range.
    # To get the requested history, we can set the date to NOW (MST) and range to hours_back.
    
    # Use MST (UTC-7) for the request date, as CAIC expects Local Mountain Time
    now_utc = datetime.datetime.utcnow()
    now_mst = now_utc - datetime.timedelta(hours=7)
    date_str = now_mst.strftime("%Y-%m-%d+%H")
    
    url = f"https://stations.avalanche.state.co.us/tabular.php?title={station_conf['name_url']}&st={station_conf['code']}&date={date_str}&unit=e&area=caic&range={hours_back}"
    
    print(f"Fetching observations for {station_conf['code']} from: {url}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Error fetching observations: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, 'html.parser')
    pre_tag = soup.find('pre')
    
    if not pre_tag:
        print("Error: Could not find <pre> tag in response.")
        return pd.DataFrame()
    
    raw_text = pre_tag.get_text()
    lines = raw_text.split('\n')
    
    data_list = []
    
    # Example Line:
    # 2025 Dec 11 09:00 am    20   24   20   19  97  28 274  38    34
    # Columns appear to be: Date(3 parts), Time(2 parts), Temp, MxTp, MnTp, DewP, RH, Spd, Dir, Gst, SWIN
    
    for line in lines:
        line = line.strip()
        if not line: continue
        if "Date" in line and "Time" in line: continue # Header
        
        # Try to parse
        try:
            # Check if line starts with a year
            if not re.match(r'^\d{4}', line): continue
            
            parts = line.split()
            # 0: YYYY, 1: MMM, 2: DD, 3: HH:MM, 4: am/pm
            # 5: Temp, 6: MxTp, 7: MnTp, 8: DewP, 9: RH, 10: Spd, 11: Dir
            
            if len(parts) < 12: continue
            
            date_str = f"{parts[0]} {parts[1]} {parts[2]} {parts[3]} {parts[4]}"
            # Parse Date - MST is UTC-7
            
            dt_local = datetime.datetime.strptime(date_str, "%Y %b %d %I:%M %p")
            dt_utc = dt_local + datetime.timedelta(hours=7)
            
            speed_mph = float(parts[10]) # Spd
            direction = float(parts[11]) # Dir
            gust_mph = float(parts[12]) if len(parts) > 12 and parts[12].replace('.','').isdigit() else speed_mph # Gst (fallback to speed if missing)
            
            # Convert MPH to m/s
            speed_mps = speed_mph * 0.44704
            gust_mps = gust_mph * 0.44704
            
            data_list.append({
                "timestamp": dt_utc,
                "speed_mps": speed_mps,
                "gust_mps": gust_mps,
                "direction_deg": direction
            })
            
        except Exception as e:
            # print(f"Skipping line '{line}': {e}")
            pass
            
    df = pd.DataFrame(data_list)
    if not df.empty:
        df.sort_values(by="timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        
    return df

if __name__ == "__main__":
    for station in STATIONS:
        df = fetch_obs(station, hours_back=48)
        if not df.empty:
            df.to_csv(station['output_file'], index=False)
            print(f"Saved {len(df)} rows to {station['output_file']}")
        else:
            print(f"No data for {station['code']}")

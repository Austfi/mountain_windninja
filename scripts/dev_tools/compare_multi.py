import os
import pandas as pd
import matplotlib.pyplot as plt
import glob
import datetime
import subprocess
import numpy as np
import sys

# Station Configuration
STATIONS = [
    {
        "name": "Keystone Wind Study",
        "code": "CAKWS",
        "lat": 39.56216, 
        "lon": -105.91444, 
        "obs_file": "obs_CAKWS.csv"
    },
    {
        "name": "Keystone Wapiti",
        "code": "CAKWP",
        "lat": 39.54505, 
        "lon": -105.91913, 
        "obs_file": "obs_CAKWP.csv"
    }
]

MPS_TO_MPH = 2.23694
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REANALYSIS_DIR = os.path.join(BASE_DIR, "temp", "reanalysis_multi")

def get_value(asc_file, lon, lat):
    cmd = ["gdallocationinfo", "-valonly", "-wgs84", asc_file, str(lon), str(lat)]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        val = float(res.stdout.strip())
        if val == -9999: return np.nan
        return val
    except:
        return np.nan

def load_obs(filepath):
    path = os.path.join(BASE_DIR, filepath) # filepath is relative name from config? Code above put it in CWD or BASE? fetch_obs puts it in CWD.
    # Actually fetch_obs saves to CWD (keystone_automation). BASE_DIR is .../keystone_automation.
    full_path = os.path.join(BASE_DIR, filepath)
    if not os.path.exists(full_path):
        # try scripts dir relative? No, fetch_obs runs in CWD.
        # Fallback to CWD
        if os.path.exists(filepath):
            full_path = filepath
        else:
            print(f"  [WARN] Obs file missing: {filepath}")
            return pd.DataFrame()
            
    df = pd.read_csv(full_path, parse_dates=['timestamp'])
    df.set_index('timestamp', inplace=True)
    # obs fetcher saves col 'speed_mps'. Rename to 'wind_speed' for consistency? 
    # Or start using 'speed_mps' everywhere. The fetcher uses 'speed_mps'.
    if 'speed_mps' in df.columns:
        df.rename(columns={'speed_mps': 'wind_speed'}, inplace=True)
    return df

def run_comparison():
    print(">>> Starting Multi-Station Comparison")
    
    # We will iterate through step dirs ONCE, and extract for ALL stations to save IO
    # Data structure: station_data[code] = list of rows
    station_data = {s['code']: [] for s in STATIONS}
    
    step_dirs = sorted(glob.glob(os.path.join(REANALYSIS_DIR, "step_*")))
    print(f"Found {len(step_dirs)} step directories.")
    
    for d in step_dirs:
        base = os.path.basename(d)
        ts_str = base.replace("step_", "")
        try:
            ts = datetime.datetime.strptime(ts_str, "%Y%m%d_%H%M")
        except:
            continue
            
        # Files
        wn_hrrr = glob.glob(os.path.join(d, "*_HRRR_vel.asc"))
        wn_nam = glob.glob(os.path.join(d, "*_NAM_vel.asc"))
        raw_hrrr = glob.glob(os.path.join(d, "hrrr_*_spd.asc"))
        raw_nam = glob.glob(os.path.join(d, "nam_*_spd.asc"))
        
        # Extract for each station
        for s in STATIONS:
            row = {'timestamp': ts}
            if wn_hrrr: row['WN-HRRR'] = get_value(wn_hrrr[0], s['lon'], s['lat'])
            if wn_nam: row['WN-NAM'] = get_value(wn_nam[0], s['lon'], s['lat'])
            if raw_hrrr: row['Raw-HRRR'] = get_value(raw_hrrr[0], s['lon'], s['lat'])
            if raw_nam: row['Raw-NAM'] = get_value(raw_nam[0], s['lon'], s['lat'])
            station_data[s['code']].append(row)
            
        # print(f"  Processed {ts_str}")

    # Plotting setup
    fig, axes = plt.subplots(len(STATIONS), 1, figsize=(14, 10), sharex=True)
    if len(STATIONS) == 1: axes = [axes]
    
    print("\n--- Statistics (MPH) ---")
    
    for idx, s in enumerate(STATIONS):
        ax = axes[idx]
        code = s['code']
        name = s['name']
        
        print(f"\nStation: {name} ({code})")
        
        df_model = pd.DataFrame(station_data[code])
        if df_model.empty:
            print("  No model data.")
            continue
            
        df_model.set_index('timestamp', inplace=True)
        
        # Load Obs
        df_obs = load_obs(s['obs_file'])
        if df_obs.empty:
            print("  No obs data.")
        
        # Merge
        merged = pd.merge_asof(
            df_model.sort_index(), 
            df_obs.sort_index(), 
            left_index=True, 
            right_index=True, 
            direction='nearest', 
            tolerance=pd.Timedelta('30min')
        )
        
        # Convert units (Everything to MPH)
        cols = ['WN-HRRR', 'WN-NAM', 'Raw-HRRR', 'Raw-NAM', 'wind_speed']
        for c in cols:
            if c in merged.columns:
                merged[c] = merged[c] * MPS_TO_MPH
                
        # Stats
        targets = ['WN-HRRR', 'WN-NAM', 'Raw-HRRR', 'Raw-NAM']
        print(f"{'Model':<10} {'Bias':<10} {'RMSE':<10}")
        if 'wind_speed' in merged.columns:
            for t in targets:
                if t in merged.columns:
                    diff = merged[t] - merged['wind_speed']
                    bias = diff.mean()
                    rmse = np.sqrt((diff**2).mean())
                    print(f"{t:<10} {bias:.2f} {rmse:.2f}")
                    
        # Plot
        if 'wind_speed' in merged.columns:
            ax.plot(merged.index.values, merged['wind_speed'].values, 'k-', linewidth=2, label='Obs')
            
        if 'WN-HRRR' in merged.columns:
            ax.plot(merged.index.values, merged['WN-HRRR'].values, 'r-', label='WN (HRRR)')
        if 'WN-NAM' in merged.columns:
            ax.plot(merged.index.values, merged['WN-NAM'].values, 'b-', label='WN (NAM)')
            
        if 'Raw-HRRR' in merged.columns:
            ax.plot(merged.index.values, merged['Raw-HRRR'].values, 'r--', alpha=0.5, label='Raw HRRR')
        if 'Raw-NAM' in merged.columns:
            ax.plot(merged.index.values, merged['Raw-NAM'].values, 'b--', alpha=0.5, label='Raw NAM')
            
        ax.set_title(f"{name} ({code})")
        ax.set_ylabel("Wind Speed (mph)")
        ax.grid(True)
        ax.legend()
        
    plt.tight_layout()
    plt.savefig("multi_model_plot.png") # Overwrite previous single plot
    print("\nSaved multi_model_plot.png (Combined)")

if __name__ == "__main__":
    run_comparison()

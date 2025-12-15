import os
import subprocess
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import re
import sys
import argparse

# Analysis Config
STATION_LAT = 39.56216
STATION_LON = -105.91444
HOURS_BACK = 48
MODELS = ["HRRR"]
# MODELS = ["HRRR", "NAM", "NBM"]  # Full comparison
OUTPUT_BASE = "/home/austin_finnell/keystone_automation/temp"
SCRIPTS_DIR = "/home/austin_finnell/keystone_automation/scripts"

def run_reanalysis(model, hours):
    """
    Runs daily_run.py in reanalysis mode for the specified model.
    """
    cmd = [
        "python3", os.path.join(SCRIPTS_DIR, "daily_run.py"),
        "--mode", "reanalysis",
        "--hours", str(hours),
        "--model", model,
        "--no-upload", # Don't push to GCS during test
        "--keep-temp"  # Important: Don't delete output!
    ]
    print(f"Running reanalysis for {model}...")
    subprocess.run(cmd, check=True)

def read_asc_value_at_coords(asc_file, target_lat, target_lon):
    # Use gdallocationinfo
    try:
        cmd = ["gdallocationinfo", "-valonly", "-wgs84", asc_file, str(target_lon), str(target_lat)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception:
        pass
    return None

def extract_point_data(run_dir, lat, lon, file_pattern="*_vel.asc", filename_regex=r'keystone_(\d{8})_(\d{4})_vel\.asc', file_type="VEL"):
    """
    Extracts point data from ASCII grids.
    file_type: "VEL" (WindNinja output, has _vel and _ang files) or "SPD" (Raw grid, has _spd and _dir files)
    """
    data_list = []
    
    # Verify directory exists
    if not os.path.exists(run_dir):
        print(f"Directory not found: {run_dir}")
        return pd.DataFrame()

    # Find files
    files = glob.glob(os.path.join(run_dir, file_pattern))
    files.sort()
    
    print(f"Found {len(files)} files in {run_dir} matching {file_pattern}")
    
    for f in files:
        # Parse timestamp
        match = re.search(filename_regex, os.path.basename(f))
        if not match: continue
        
        try:
            groups = match.groups()
            if len(groups) == 2: # Date, Time(HHMM or HH)
                d_str = groups[0]
                t_str = groups[1]
                if len(t_str) == 2: t_str += "00" # Handle "08" -> "0800"
                dt = datetime.datetime.strptime(d_str + t_str, "%Y%m%d%H%M")
            else:
                continue
        except ValueError:
            continue
            
        # Determine speed and direction filenames
        if file_type == "VEL":
            speed_file = f
            dir_file = f.replace("_vel.asc", "_ang.asc")
        elif file_type == "SPD":
            speed_file = f
            dir_file = f.replace("_spd.asc", "_dir.asc")
        elif file_type == "GST":
            speed_file = f
            dir_file = None
        else:
            continue
        
        speed = read_asc_value_at_coords(speed_file, lat, lon)
        direction = read_asc_value_at_coords(dir_file, lat, lon) if dir_file else np.nan
        
        if speed is not None:
             data_list.append({
                "timestamp": dt,
                "speed_mps": speed,
                "direction_deg": direction
            })
            
    df = pd.DataFrame(data_list)
    if not df.empty:
        df.sort_values(by="timestamp", inplace=True)
        
    return df

def main():
    parser = argparse.ArgumentParser(description="Run reanalysis and compare with observations.")
    parser.add_argument("--hours", type=int, default=48, help="Number of hours to look back (default: 48)")
    parser.add_argument("--models", nargs="+", default=["HRRR"], help="Models to run (default: HRRR)")
    args = parser.parse_args()

    hours_back = args.hours
    models_to_run = args.models

    # 1. Fetch Observations
    # CAIC site might return empty if range is too small (<6h). 
    # Fetch safe minimum (12h) and filter.
    fetch_hours = max(12, hours_back)
    print(f"Fetching observations for last {fetch_hours} hours (requested {hours_back})...")
    
    try:
        sys.path.append(SCRIPTS_DIR)
        from fetch_obs import fetch_obs
        obs_df = fetch_obs(hours_back=fetch_hours)
        obs_df['timestamp'] = pd.to_datetime(obs_df['timestamp'])
        
        # Filter to requested hours
        cutoff_time = datetime.datetime.utcnow() - datetime.timedelta(hours=hours_back)
        obs_df = obs_df[obs_df['timestamp'] >= cutoff_time]
        
    except Exception as e:
        print(f"Failed to fetch observations: {e}")
        return

    model_data = {}

    # 2. Run Models & Extract
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    
    for model in models_to_run:
        print(f"\n--- Processing {model} ---")
        try:
            # Pass hours directly to run_reanalysis function
            run_reanalysis(model, hours_back)
            
            # Find output dir
            # daily_run: final_output_dir = .../OUTPUT_BASE/date_str_reanalysis_model
            # Note: reanalysis mode usually looks "back" 12 hours from "now".
            # The folder produced assumes "today's" date generally.
            output_dir = os.path.join(OUTPUT_BASE, f"{date_str}_reanalysis_{model}")
            
            if os.path.exists(output_dir):
                # Standard WindNinja Output
                df = extract_point_data(output_dir, STATION_LAT, STATION_LON)
                model_data[f"{model} (WindNinja)"] = df
                print(f"Extracted {len(df)} points for {model} (WindNinja)")
                
                # If HRRR, also extract raw grids
                if model == "HRRR":
                    raw_grids_dir = os.path.join(output_dir, "grids")
                    if os.path.exists(raw_grids_dir):
                        df_raw = extract_point_data(
                            raw_grids_dir, 
                            STATION_LAT, 
                            STATION_LON, 
                            file_pattern="hrrr_*_spd.asc", 
                            filename_regex=r'hrrr_(\d{8})_(\d{2})z_f00_spd\.asc', 
                            file_type="SPD"
                        )
                        model_data[f"{model} (Raw Input)"] = df_raw
                        print(f"Extracted {len(df_raw)} points for {model} (Raw Input)")
                        
                        df_gst = extract_point_data(
                            raw_grids_dir, 
                            STATION_LAT, 
                            STATION_LON, 
                            file_pattern="hrrr_*_gst.asc", 
                            filename_regex=r'hrrr_(\d{8})_(\d{2})z_f00_gst\.asc', 
                            file_type="GST"
                        )
                        model_data[f"{model} Gust (Raw Input)"] = df_gst
                        print(f"Extracted {len(df_gst)} points for {model} Gust (Raw Input)")
                        
            else:
                print(f"Output directory not found: {output_dir}")
                
        except Exception as e:
            print(f"Error processing {model}: {e}")

    # 3. Compare & Plot (convert to mph for display)
    MPS_TO_MPH = 2.23694
    
    plt.figure(figsize=(12, 6))
    
    # Plot Obs (convert to mph)
    plt.plot(obs_df['timestamp'].values, obs_df['speed_mps'].values * MPS_TO_MPH, label='Observed Speed (CAIC)', color='black', linewidth=2, marker='o', markersize=3)
    
    # Plot Gusts if available
    if 'gust_mps' in obs_df.columns:
        plt.plot(obs_df['timestamp'].values, obs_df['gust_mps'].values * MPS_TO_MPH, label='Observed Gust (CAIC)', color='gray', linewidth=1, linestyle=':', marker='^', markersize=2)
    
    stats = []
    
    for model, df in model_data.items():
        if df.empty: continue
        
        # Filter to requested window to match observations and user request
        df = df[df['timestamp'] >= cutoff_time]
        if df.empty: continue
        
        plt.plot(df['timestamp'].values, df['speed_mps'].values * MPS_TO_MPH, label=f'{model}', linestyle='--')
        
        # Merge for stats
        # align timestamps
        merged = pd.merge_asof(
            df.sort_values('timestamp'), 
            obs_df.sort_values('timestamp'), 
            on='timestamp', 
            direction='nearest', 
            tolerance=pd.Timedelta('30min')
        ).dropna()
        
        if not merged.empty:
            # Stats in mph
            bias = np.mean(merged['speed_mps_x'] - merged['speed_mps_y']) * MPS_TO_MPH # Model - Obs
            rmse = np.sqrt(np.mean((merged['speed_mps_x'] - merged['speed_mps_y'])**2)) * MPS_TO_MPH
            corr = np.corrcoef(merged['speed_mps_x'], merged['speed_mps_y'])[0,1]
            
            stats.append({
                "Model": model,
                "Bias (mph)": round(bias, 2),
                "RMSE (mph)": round(rmse, 2),
                "Corr": round(corr, 3) if not np.isnan(corr) else np.nan,
                "N": len(merged)
            })
    
    plt.title(f"Wind Speed Comparison - Keystone SA ({date_str})")
    plt.ylabel("Wind Speed (mph)")
    plt.xlabel("Date/Time (UTC)")
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    
    plot_path = f"comparison_plot_{date_str}.png"
    plt.savefig(plot_path)
    print(f"Saved plot to {plot_path}")
    
    print("\nStatistical Summary:")
    print(pd.DataFrame(stats))

if __name__ == "__main__":
    main()

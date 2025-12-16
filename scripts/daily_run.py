#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess
import shutil
import glob
import math
import zipfile
import requests
import time
import re

# Add current directory to path to ensure local module imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import utils
import config_loader
from gcs_manager import manager as gcs
import create_time_series

# Setup Logging
logger = utils.setup_logging("daily_run")

def download_hrrr_single_hour(target_dt_utc, output_dir):
    """
    Downloads the specific HRRR GRIB2 file for a target hour (valid time).
    Searches backwards from 'now' to find the latest available cycle.
    """
    utils.ensure_dir(output_dir)

    current_utc = datetime.datetime.utcnow()
    target_cycle = target_dt_utc.replace(minute=0, second=0, microsecond=0)
    
    cycles_to_try = []
    
    # Priority 1: The cycle matching the target time (analysis or f00) 
    if target_cycle <= current_utc:
         cycles_to_try.append((target_cycle, 0)) # f00
    
    # Fallback: previous cycles with increasing forecast hours
    for back_hours in range(1, 19):  # f01 through f18
        prev_cycle = target_cycle - datetime.timedelta(hours=back_hours)
        cycles_to_try.append((prev_cycle, back_hours))
    
    for cycle_time, f_hour in cycles_to_try:
        date_str = cycle_time.strftime("%Y%m%d")
        hh = cycle_time.strftime("%H")
        ff = f"{f_hour:02d}"
        
        filename = f"hrrr.t{hh}z.wrfsfcf{ff}.grib2"
        url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{date_str}/conus/{filename}"
        
        local_path = os.path.join(output_dir, f"hrrr_{date_str}_{hh}z_f{ff}.grib2")
        
        if os.path.exists(local_path):
            logger.info(f"File already exists: {local_path}")
            return local_path
            
        logger.info(f"Attempting download: {url}")
        try:
            r = requests.get(url, stream=True, timeout=10)
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info("Download successful.")
                return local_path
            elif r.status_code == 404:
                logger.warning(f"404 Not Found on NOMADS (Cycle {hh}z F{ff}). Checking GCS archive...")
                
                # Fallback to Google Cloud Storage Public Dataset
                gcs_url = f"https://storage.googleapis.com/high-resolution-rapid-refresh/hrrr.{date_str}/conus/{filename}"
                logger.info(f"Attempting GCS: {gcs_url}")
                
                try:
                    r2 = requests.get(gcs_url, stream=True, timeout=15)
                    if r2.status_code == 200:
                        with open(local_path, 'wb') as f:
                            for chunk in r2.iter_content(chunk_size=8192):
                                f.write(chunk)
                        logger.info("Download successful (GCS).")
                        return local_path
                except Exception as e2:
                    logger.error(f"GCS Download Error: {e2}")

            else:
                logger.warning(f"HTTP {r.status_code}. Retrying...")
        except Exception as e:
            logger.error(f"Download error: {e}")
            
    raise Exception(f"Could not find valid HRRR data for {target_dt_utc}")

def download_nam_single_hour(target_dt_utc, output_dir):
    """Downloads NAM Conus Nest GRIB2 key for a target hour."""
    utils.ensure_dir(output_dir)
    
    hour = target_dt_utc.hour
    cycle = (hour // 6) * 6
    offset = hour - cycle
    ymd = target_dt_utc.strftime("%Y%m%d")
    
    filename = f"nam.t{cycle:02d}z.conusnest.hiresf{offset:02d}.tm00.grib2"
    url = f"https://noaa-nam-pds.s3.amazonaws.com/nam.{ymd}/{filename}"
    local_path = os.path.join(output_dir, f"nam_{ymd}_{hour:02d}z.grib2")
    
    if os.path.exists(local_path):
        return local_path
        
    logger.info(f"Attempting download (NAM via AWS): {url}")
    try:
        r = requests.get(url, stream=True, timeout=30)
        if r.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return local_path
        else:
            logger.warning(f"HTTP {r.status_code} for {url}")
            return None
    except Exception as e:
        logger.error(f"Download error: {e}")
        return None

def download_nam_forecast_hour(target_dt_utc, output_dir):
    """Downloads NAM GRIB2 for a target hour using the BEST AVAILABLE previous cycle."""
    utils.ensure_dir(output_dir)
    now_utc = datetime.datetime.utcnow()
    
    potential_cycles = []
    start_search = now_utc.replace(minute=0, second=0, microsecond=0)
    for i in range(24): 
        t = start_search - datetime.timedelta(hours=i)
        if t.hour % 6 == 0:
            potential_cycles.append(t)
            
    for cycle in potential_cycles:
        diff = (target_dt_utc - cycle).total_seconds() / 3600
        if diff < 0 or diff > 60: continue 
        f_hour = int(diff)
        
        ymd = cycle.strftime("%Y%m%d")
        cc = cycle.strftime("%H")
        filename = f"nam.t{cc}z.conusnest.hiresf{f_hour:02d}.tm00.grib2"
        url = f"https://noaa-nam-pds.s3.amazonaws.com/nam.{ymd}/{filename}"
        local_path = os.path.join(output_dir, f"nam_{ymd}_{cc}z_f{f_hour:02d}.grib2")
        
        if os.path.exists(local_path): return local_path
             
        logger.info(f"Probing NAM Cycle {cc}z f{f_hour:02d} for target {target_dt_utc}...")
        try:
             r = requests.get(url, stream=True, timeout=10)
             if r.status_code == 200:
                 with open(local_path, 'wb') as f:
                     for chunk in r.iter_content(chunk_size=8192):
                         f.write(chunk)
                 return local_path
        except:
             pass
             
    logger.error(f"Failed to find NAM forecast for {target_dt_utc}")
    return None

def generate_config(date_str, start_time, stop_time, weather_model_path=None, sub_dir=None, wx_model_type_override=None):
    """Reads the template config and generates a run config."""
    # Choose template based on domain setting in ConfigLoader or passed arg?
    # Logic: daily_run arg -> sets global template var. 
    # We'll use the one set in main() via global CONFIG_TEMPLATE for now to min diff.
    
    run_output_dir = sub_dir if sub_dir else os.path.join(config_loader.TEMP_DIR, date_str)
    utils.ensure_dir(run_output_dir)
    
    with open(CONFIG_TEMPLATE, 'r') as f:
        config_content = f.read()
    
    duration_delta = stop_time - start_time
    duration_hours = max(1, int(duration_delta.total_seconds() / 3600))

    filled_config = config_content.format(
        start_year=start_time.year, start_month=start_time.month, start_day=start_time.day,
        start_hour=start_time.hour, start_minute=start_time.minute,
        stop_year=stop_time.year, stop_month=stop_time.month, stop_day=stop_time.day,
        stop_hour=stop_time.hour, stop_minute=stop_time.minute,
        forecast_duration=duration_hours
    )
    
    # Inject extra config lines
    lines = filled_config.split('\n')
    new_lines = []
    found_output = False
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('output_path'):
            new_lines.append(f"""output_wind_height = 10.0
units_output_wind_height = m
output_speed_units = mps
write_ascii_output = true
write_goog_output = true
output_path = {run_output_dir}""")
            found_output = True
        elif wx_model_type_override == "GRIDDED" and stripped.startswith(('diurnal_winds', 'initialization_method', 'wx_model_type')):
             new_lines.append(f"# {stripped} (Disabled for GRIDDED init)")
             if stripped.startswith('initialization_method'):
                 new_lines.append("initialization_method = griddedInitialization")
        elif weather_model_path and stripped.startswith('initialization_method'):
            new_lines.append("initialization_method = wxModelInitialization")
        # Remove wx_model_type for local file batch mode to avoid conflicts
        elif weather_model_path and stripped.startswith('wx_model_type'):
            pass  # Don't include wx_model_type when using local files
        elif stripped.startswith('weather_model_file') and weather_model_path:
            pass # Skip existing, injected later
        elif stripped.startswith(('output_wind_height', 'units_output_wind_height', 'output_speed_units', 'write_ascii_output', 'write_goog_output', 'output_path')):
            pass # These are injected at the end
        else:
            new_lines.append(line)
            
    # Always append required output configuration at the end
    new_lines.append(f"""
output_wind_height = 10.0
units_output_wind_height = m
output_speed_units = mph
write_ascii_output = true
write_goog_output = true
output_path = {run_output_dir}""")
        
    config_path = os.path.join(run_output_dir, f"keystone_{start_time.strftime('%Y%m%d_%H%M')}.cfg")
    with open(config_path, 'w') as f:
        f.write('\n'.join(new_lines))
        
    return config_path, run_output_dir

def run_windninja(config_path, extra_args=None):
    cmd = [config_loader.WINDNINJA_CLI, config_path]
    if extra_args: cmd.extend(extra_args)
        
    logger.info(f"Running WindNinja: {' '.join(cmd)}")
    config_basename = os.path.splitext(os.path.basename(config_path))[0]
    output_dir = os.path.dirname(config_path)
    
# --- HARD RESET OpenFOAM case before every run ---
    case_dir = os.path.join(
    	config_loader.BASE_DIR,
	"static_data",
    	f"NINJAFOAM_{config_basename}"
    )

    if os.path.exists(case_dir):
    	logger.warning(f"Removing existing WindNinja case directory: {case_dir}")
    	shutil.rmtree(case_dir)

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running WindNinja: {e}")
        raise

def archive_results(run_output_dir, archive_name_base):
    utils.ensure_dir(config_loader.ARCHIVE_DIR)
    archive_path = os.path.join(config_loader.ARCHIVE_DIR, f"{archive_name_base}.zip")
    
    grids_dir = os.path.join(run_output_dir, "grids")
    if os.path.exists(grids_dir): shutil.rmtree(grids_dir)
        
    logger.info(f"Archiving results to {archive_path}...")
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(run_output_dir):
            for file in files:
                if file.endswith(".kmz"):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, run_output_dir)
                    zipf.write(file_path, arcname)

    shutil.rmtree(run_output_dir)
    return archive_path

def enforce_retention_policy(days=7):
    """Deletes archives older than 'days'."""
    now = datetime.datetime.now()
    retention_delta = datetime.timedelta(days=days)
    logger.info("Enforcing retention policy...")
    
    if os.path.exists(config_loader.ARCHIVE_DIR):
        for filename in os.listdir(config_loader.ARCHIVE_DIR):
            file_path = os.path.join(config_loader.ARCHIVE_DIR, filename)
            if os.path.isfile(file_path) and now - datetime.datetime.fromtimestamp(os.path.getmtime(file_path)) > retention_delta:
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted old archive: {filename}")
                except OSError as e:
                    logger.error(f"Error deleting {filename}: {e}")

    # GRIB Cache Cleanup
    grib_cache_dir = os.path.join(config_loader.TEMP_DIR, "grib_cache")
    if os.path.exists(grib_cache_dir):
        cache_retention = datetime.timedelta(days=2)
        for filename in os.listdir(grib_cache_dir):
             file_path = os.path.join(grib_cache_dir, filename)
             if os.path.isfile(file_path) and now - datetime.datetime.fromtimestamp(os.path.getmtime(file_path)) > cache_retention:
                 try: os.remove(file_path)
                 except: pass

def get_run_parameters(mode, hours, target_date_str=None):
    now_utc = datetime.datetime.utcnow()
    
    if mode == "full_forecast":
        start = now_utc.replace(minute=0, second=0, microsecond=0)
        stop = start + datetime.timedelta(hours=hours)
        label = f"forecast_{hours}h"
    elif mode == "am_forecast":
        start = now_utc.replace(hour=7, minute=0, second=0, microsecond=0)
        stop = start + datetime.timedelta(hours=6)
        label = "am_forecast"
    elif mode == "pm_forecast":
        start = now_utc.replace(hour=19, minute=0, second=0, microsecond=0)
        stop = start + datetime.timedelta(hours=6)
        label = "pm_forecast"
    elif mode == "reanalysis":
        # Reanalysis: Last N hours from current time (stays within NOMADS 24h window)
        stop = now_utc.replace(minute=0, second=0, microsecond=0)
        start = stop - datetime.timedelta(hours=hours)
        label = f"reanalysis_{hours}h"
    elif mode == "forecast":
        start = now_utc.replace(minute=0, second=0, microsecond=0)
        stop = start + datetime.timedelta(hours=hours)
        label = f"forecast_{hours}h"
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return {
        'start': start,
        'stop': stop,
        'label': label,
        'type': 'reanalysis' if 'reanalysis' in mode else 'forecast'
    }

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full_forecast", "am_forecast", "pm_forecast", "reanalysis", "forecast"], default="full_forecast")
    parser.add_argument("--hours", type=int, default=12)
    parser.add_argument("--test-duration", type=int)
    parser.add_argument("--date", type=str)
    parser.add_argument("--domain", choices=["small", "large"], default="small")
    parser.add_argument("--model", choices=["HRRR", "NBM", "NAM"], default="HRRR")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()
    
    # Global template selection
    global CONFIG_TEMPLATE
    CONFIG_TEMPLATE = str(config_loader.DOMAIN_TEMPLATES[args.domain])
    logger.info(f"Domain: {args.domain.upper()} ({CONFIG_TEMPLATE})")
    
    MODEL_MAP = {
        "HRRR": "NOMADS-HRRR-CONUS-3-KM",
        "NBM": "NOMADS-NBM-CONUS-2.5-KM",
        "NAM": "NOMADS-NAM-NEST-CONUS-3-KM"
    }
    selected_wx_model = MODEL_MAP[args.model]

    # Ensure import visibility for dynamic script
    try:
        import create_time_series
    except ImportError:
        logger.warning("Could not import create_time_series. Post-processing may fail.")

    run_params = get_run_parameters(args.mode, args.hours, args.date)
    
    if args.test_duration:
        run_params['stop'] = run_params['start'] + datetime.timedelta(hours=args.test_duration)

    date_str = datetime.datetime.now().strftime("%Y%m%d")
    final_output_dir = os.path.join(config_loader.TEMP_DIR, f"{date_str}_{run_params['label']}_{args.model}")
    
    logger.info(f"Starting Mode: {args.mode.upper()}")
    logger.info(f"Model: {args.model}")
    logger.info(f"Window (UTC): {run_params['start']} to {run_params['stop']}")
    
    if config_loader.GCS_UPLOAD_ENABLED and not args.dry_run and not args.no_upload:
        gcs.upload_status(run_params['label'], args.model, "running")
        gcs.update_index()

    try:
        utils.ensure_dir(final_output_dir)
        
        # FORECAST MODE: Use WindNinja's native NOMADS downloader
        # REANALYSIS MODE: Use batch mode with pre-downloaded f00 files
        if run_params['type'] == 'forecast':
            # Generate config with native wx_model_type - WindNinja downloads HRRR
            logger.info(f"--- Forecast Mode: Using WindNinja native NOMADS download ---")
            config_path, _ = generate_config(
                date_str, run_params['start'], run_params['stop'],
                weather_model_path=None,  # No external file - let WindNinja download
                wx_model_type_override=None,  # Keep native wx_model_type from template
                sub_dir=final_output_dir
            )
            
            if not args.dry_run:
                run_windninja(config_path)
        else:
            # REANALYSIS: Also use WindNinja's native NOMADS downloader for historical data
            # WindNinja can access historic HRRR data from the NOMADS archive
            logger.info(f"--- Reanalysis Mode: Using WindNinja native NOMADS download ---")
            config_path, _ = generate_config(
                date_str, run_params['start'], run_params['stop'],
                weather_model_path=None,  # Let WindNinja download historic data
                wx_model_type_override=None,  # Keep native wx_model_type from template  
                sub_dir=final_output_dir
            )
            
            if not args.dry_run:
                run_windninja(config_path)
            
        # Post-Processing: Rename Outputs (for reanalysis batch mode)
        if run_params['type'] == 'reanalysis':
            logger.info("Post-processing batch outputs...")
            all_vels = glob.glob(os.path.join(final_output_dir, "*_vel.asc"))
            import re
            for fpath in all_vels:
                 fname = os.path.basename(fpath)
                 # Regex for YYYYMMDD_HHMM or similar patterns from WindNinja
                 # Heuristic: Match 10-12 digits
                 match_iso = re.search(r'(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})', fname)
                 if match_iso:
                     ymd_hm = f"{match_iso.group(1)}{match_iso.group(2)}{match_iso.group(3)}_{match_iso.group(4)}{match_iso.group(5)}"
                     new_name = f"keystone_{ymd_hm}_vel.asc"
                     try: os.rename(fpath, os.path.join(final_output_dir, new_name))
                     except: pass
                     
                     # Rename related files
                     base_old = fpath.replace("_vel.asc", "")
                     base_new = os.path.join(final_output_dir, f"keystone_{ymd_hm}")
                     for ext in ["_ang.asc", "_vel.prj", "_ang.prj", "_vel.asc.aux.xml", "_80m.kmz"]:
                         if os.path.exists(base_old + ext):
                             try: os.rename(base_old + ext, base_new + ext)
                             except: pass

            # Rename Batch KMZs
            all_kmzs = glob.glob(os.path.join(final_output_dir, "*.kmz"))
            for kpath in all_kmzs:
                kname = os.path.basename(kpath)
                m = re.search(r'(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})', kname)
                if m:
                     yyyy, mm, dd, hh, min_ = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
                     ts_str = f"{mm}-{dd}-{yyyy}_{hh}{min_}"
                     new_kmz = f"keystone_square_30m_{ts_str}_80m.kmz"
                     try: os.rename(kpath, os.path.join(final_output_dir, new_kmz))
                     except: pass

        # Final Archival and Upload
        if not args.dry_run:
            output_base_name = f"keystone_{run_params['label']}_{args.model}_{date_str}"
            
            # 1. Generate single playable KMZ (bundled with all hours)
            playable_kmz_path = None
            try:
                playable_kmz_path = create_time_series.create_playable_kmz(final_output_dir, output_base_name)
                logger.info(f"Generated playable KMZ: {playable_kmz_path}")
            except Exception as e:
                logger.error(f"Playable KMZ creation failed: {e}")
            
            if config_loader.GCS_UPLOAD_ENABLED and not args.no_upload:
                gcs.upload_status(run_params['label'], args.model, "success")
                
                # 2. Upload playable KMZ to bucket ROOT as latest_*.kmz (OVERWRITE)
                if playable_kmz_path and os.path.exists(playable_kmz_path):
                    if "reanalysis" in run_params['label']:
                        latest_kmz_name = "latest_reanalysis.kmz"
                    else:
                        latest_kmz_name = "latest_forecast.kmz"
                    
                    gcs.upload_file(playable_kmz_path, latest_kmz_name, cache_control="public, max-age=60")
                    logger.info(f"Uploaded to gs://{config_loader.GCS_BUCKET}/{latest_kmz_name}")

            # 3. Archive results
            if not args.keep_temp:
                archive_name = f"keystone_{run_params['label']}_{args.model}_{date_str}"
                archive_path = archive_results(final_output_dir, archive_name)
                
                if config_loader.GCS_UPLOAD_ENABLED and not args.no_upload and os.path.exists(archive_path):
                     # gs://bucket/archives/YYYY-MM-DD/file.zip
                     dest_path = f"archives/{datetime.datetime.now().strftime('%Y-%m-%d')}/{os.path.basename(archive_path)}"
                     gcs.upload_file(archive_path, dest_path)
                     gcs.update_index()
                     gcs.cleanup_old_forecasts()
                
                enforce_retention_policy()
        
        logger.info("Workflow completed.")

    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {e}")
        if config_loader.GCS_UPLOAD_ENABLED and not args.dry_run and not args.no_upload:
             gcs.upload_status(run_params['label'], args.model, "failure", error=str(e))
             gcs.update_index()
        raise

if __name__ == "__main__":
    main()

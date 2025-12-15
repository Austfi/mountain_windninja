#!/usr/bin/env python3
"""
GCS Upload Module for WindNinja Forecasts

Handles uploading forecast archives to Google Cloud Storage bucket.
Uses gsutil for reliable uploads with retry logic.
"""

import os
import subprocess
import json
import datetime
from pathlib import Path

# GCS Configuration
GCS_BUCKET = "wrf-austin-bucket"
GCS_PUBLIC_URL_BASE = f"https://storage.googleapis.com/{GCS_BUCKET}"

def upload_file(local_path, bucket_name, dest_path, retries=3):
    """
    Upload a single file to GCS using gsutil.
    
    Args:
        local_path: Path to local file
        bucket_name: GCS bucket name (without gs://)
        dest_path: Destination path within bucket
        retries: Number of retry attempts
        
    Returns:
        True if successful, False otherwise
    """
    if not os.path.exists(local_path):
        print(f"ERROR: Local file not found: {local_path}")
        return False
        
    gs_uri = f"gs://{bucket_name}/{dest_path}"
    
    for attempt in range(retries):
        try:
            cmd = ["gsutil", "-q", "cp", local_path, gs_uri]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print(f"Uploaded: {local_path} -> {gs_uri}")
                return True
            else:
                print(f"Upload attempt {attempt + 1} failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"Upload timeout on attempt {attempt + 1}")
        except Exception as e:
            print(f"Upload error on attempt {attempt + 1}: {e}")
            
    print(f"FAILED to upload after {retries} attempts: {local_path}")
    return False


def get_run_metadata(run_label, archive_date_str=None):
    """
    Get metadata about run times and coverage periods.
    
    Returns dict with:
        - run_time_utc: When the model was run
        - start_utc, stop_utc: Coverage period in UTC
        - start_mst, stop_mst: Coverage period in MST (UTC-7)
        - description: Human-readable description
    """
    from datetime import datetime, timedelta
    
    # Parse archive date or use today
    if archive_date_str:
        try:
            base_date = datetime.strptime(archive_date_str, "%Y%m%d")
        except:
            base_date = datetime.utcnow()
    else:
        base_date = datetime.utcnow()
    
    base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    metadata = {
        "run_time_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }
    
    if run_label == "pm_forecast":
        # PM forecast: 22:00 UTC (15:00 MST) for 15 hours
        start = base_date.replace(hour=22)
        stop = start + timedelta(hours=15)
        metadata.update({
            "start_utc": start.strftime("%Y-%m-%d %H:%M"),
            "stop_utc": stop.strftime("%Y-%m-%d %H:%M"),
            "start_mst": (start - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "stop_mst": (stop - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "description": "Evening through morning forecast"
        })
    elif run_label == "am_forecast":
        # AM forecast: 13:00 UTC (06:00 MST) for 12 hours
        start = base_date.replace(hour=13)
        stop = start + timedelta(hours=12)
        metadata.update({
            "start_utc": start.strftime("%Y-%m-%d %H:%M"),
            "stop_utc": stop.strftime("%Y-%m-%d %H:%M"),
            "start_mst": (start - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "stop_mst": (stop - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "description": "Daytime forecast"
        })
    elif run_label == "reanalysis":
        # Reanalysis: 00:00-12:00 UTC (17:00-05:00 MST previous night)
        start = base_date.replace(hour=0)
        stop = base_date.replace(hour=12)
        metadata.update({
            "start_utc": start.strftime("%Y-%m-%d %H:%M"),
            "stop_utc": stop.strftime("%Y-%m-%d %H:%M"),
            "start_mst": (start - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "stop_mst": (stop - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"),
            "description": "Overnight historical analysis"
        })
    else:
        # Generic/test
        metadata.update({
            "start_utc": "Variable",
            "stop_utc": "Variable",
            "start_mst": "Variable",
            "stop_mst": "Variable",
            "description": "On-demand run"
        })
    
    return metadata


def upload_archive(archive_path, run_label, model, bucket_name=GCS_BUCKET):
    """
    Upload a forecast archive to GCS with organized path structure.
    
    Structure: gs://bucket/YYYY-MM-DD/run_label_model/archive.zip
    
    Args:
        archive_path: Path to the .zip archive
        run_label: Run type (pm_forecast, am_forecast, reanalysis)
        model: Model name (HRRR, NAM, NBM)
        bucket_name: GCS bucket name
        
    Returns:
        Public URL if successful, None otherwise
    """
    if not os.path.exists(archive_path):
        print(f"Archive not found: {archive_path}")
        return None
        
    # Organize by date
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    archive_name = os.path.basename(archive_path)
    
    # Extract date from archive name if present (keystone_xxx_20251211.zip)
    import re
    date_match = re.search(r'(\d{8})\.zip$', archive_name)
    archive_date_str = date_match.group(1) if date_match else None
    
    # Destination path: YYYY-MM-DD/run_label_MODEL/filename.zip
    dest_path = f"{today}/{run_label}_{model}/{archive_name}"
    
    if upload_file(archive_path, bucket_name, dest_path):
        public_url = f"{GCS_PUBLIC_URL_BASE}/{dest_path}"
        return public_url
    return None


def upload_latest_forecast(local_path, model, run_type="forecast", bucket_name=GCS_BUCKET):
    """
    Uploads the provided KMZ to a stable 'latest' URL, overwriting the previous one.
    e.g. gs://bucket/latest/HRRR_latest.kmz or HRRR_reanalysis_latest.kmz
    """
    dest_path = f"latest/{model}_latest.kmz"
    
    # Check checks handled by caller (daily_run) usually, or we can just proceed.
    # if not GCS_AVAILABLE: return None # Removed due to NameError
        
    # Determine destination filename based on run_type
    if run_type == "reanalysis":
        dest_path = f"latest/{model}_reanalysis_latest.kmz"
    else:
        dest_path = f"latest/{model}_latest.kmz"
    
    if not os.path.exists(local_path):
        print(f"File not found: {local_path}")
        return None
        
    # We want to use specific headers for this file to prevent aggressive caching
    # upload_file uses generic gsutil cp. 
    # Let's write a specific gsutil call here or reuse upload_file if we don't care about headers yet.
    # To be safe and simple, let's reuse upload_file for now.
    # If we need headers, we can modify upload_file or do it here.
    
    # Actually, let's try to add headers via a direct call since upload_file is simple.
    gs_uri = f"gs://{bucket_name}/{dest_path}"
    print(f"Uploading LATEST forecast to {gs_uri}...")
    
    try:
        # -h "Cache-Control:public, max-age=300" -> 5 minutes cache
        cmd = ["gsutil", "-h", "Cache-Control:public, max-age=300", "cp", local_path, gs_uri]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            public_url = f"{GCS_PUBLIC_URL_BASE}/{dest_path}"
            print(f"Latest upload successful: {public_url}")
            return public_url
        else:
            print(f"Latest upload failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"Latest upload error: {e}")
        return None



def upload_status(run_label, model, status, error=None, bucket_name=GCS_BUCKET):
    """
    Upload a status JSON file to GCS.
    
    Args:
        run_label: Run type (pm_forecast, am_forecast, reanalysis)
        model: Model name
        status: "success", "failure", or "running"
        error: Optional error message
    """
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"status_{run_label}_{model}.json"
    dest_path = f"{today}/{filename}"
    
    status_data = {
        "status": status,
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
        "run_type": run_label,
        "model": model,
        "error": error
    }
    
    temp_path = f"/tmp/{filename}"
    with open(temp_path, 'w') as f:
        json.dump(status_data, f, indent=2)
        
    return upload_file(temp_path, bucket_name, dest_path)

def update_index(bucket_name=GCS_BUCKET):
    """
    Generate and upload an index.json listing all available forecasts and statuses.
    """
    try:
        # List all zip objects
        cmd_zip = ["gsutil", "ls", "-r", f"gs://{bucket_name}/**/*.zip"]
        result_zip = subprocess.run(cmd_zip, capture_output=True, text=True, timeout=60)
        
        # List all status json objects (look in recent date folders to save time?)
        # For now, full recursive listing is safest/easiest if not too large
        cmd_status = ["gsutil", "ls", "-r", f"gs://{bucket_name}/**/status_*.json"]
        result_status = subprocess.run(cmd_status, capture_output=True, text=True, timeout=60)
        
        files = []
        if result_zip.returncode == 0:
            files.extend([f for f in result_zip.stdout.strip().split('\n') if f])
            
        status_files = []
        if result_status.returncode == 0:
            status_files.extend([f for f in result_status.stdout.strip().split('\n') if f])
        
        # Parse into structured data
        index_data = {
            "updated": datetime.datetime.utcnow().isoformat() + "Z",
            "bucket": bucket_name,
            "base_url": GCS_PUBLIC_URL_BASE,
            "forecasts": [],
            "statuses": {} # Keyed by "YYYY-MM-DD_type_model"
        }
        
        # Process ZIPs
        for gs_uri in files:
            path = gs_uri.replace(f"gs://{bucket_name}/", "")
            parts = path.split("/")
            
            if len(parts) >= 3:
                date = parts[0]
                run_info = parts[1]
                filename = parts[-1]
                
                run_parts = run_info.rsplit("_", 1)
                if len(run_parts) == 2:
                    run_label = run_parts[0]
                    model = run_parts[1]
                else:
                    run_label = run_info
                    model = "unknown"
                
                index_data["forecasts"].append({
                    "date": date,
                    "run_type": run_label,
                    "model": model,
                    "filename": filename,
                    "url": f"{GCS_PUBLIC_URL_BASE}/{path}"
                })
                
        # Process Statuses (Download content for recent ones?)
        # Downloading every status file is slow. 
        # Optimized approach: Since we only need the filename to know it exists? 
        # No, we need the CONTENT (status=success/fail).
        # We can use 'cat' on recent ones.
        
        # Filter status files to last 3 days to avoid slow re-indexing
        today = datetime.datetime.utcnow()
        recent_cutoff = today - datetime.timedelta(days=3)
        
        files_to_read = []
        for gs_uri in status_files:
             # Check date in path
             # gs://bucket/YYYY-MM-DD/...
             try:
                 parts = gs_uri.replace(f"gs://{bucket_name}/", "").split('/')
                 file_date = datetime.datetime.strptime(parts[0], "%Y-%m-%d")
                 if file_date >= recent_cutoff:
                     files_to_read.append(gs_uri)
             except:
                 continue
                 
        if files_to_read:
            # Batch cat?
            # gsutil cat gs://... gs://...
            # Might be too many args. Do one by one or in small batches.
            for uri in files_to_read:
                try:
                    res = subprocess.run(["gsutil", "cat", uri], capture_output=True, text=True, timeout=5)
                    if res.returncode == 0:
                        data = json.loads(res.stdout)
                        # Key: YYYY-MM-DD_type_model
                        # Extract date from uri (safest)
                        uri_parts = uri.replace(f"gs://{bucket_name}/", "").split('/')
                        date_str = uri_parts[0]
                        key = f"{date_str}_{data.get('run_type')}_{data.get('model')}"
                        index_data["statuses"][key] = data
                except Exception as e:
                    print(f"Error reading status {uri}: {e}")
        
        # Sort forecasts
        index_data["forecasts"].sort(
            key=lambda x: (x["date"], x["run_type"], x["model"]),
            reverse=True
        )
        
        # Write/Upload
        index_path = "/tmp/gcs_index.json"
        with open(index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
            
        success_json = upload_file(index_path, bucket_name, "index.json")
        success_html = upload_html_interface(bucket_name)
        return success_json and success_html
        
    except Exception as e:
        print(f"Error updating index: {e}")
        return False


def list_available_forecasts(bucket_name=GCS_BUCKET, date=None):
    """
    List available forecasts, optionally filtered by date.
    
    Args:
        bucket_name: GCS bucket name
        date: Optional date string (YYYY-MM-DD) to filter
        
    Returns:
        List of forecast info dicts
    """
    try:
        if date:
            pattern = f"gs://{bucket_name}/{date}/**/*.zip"
        else:
            pattern = f"gs://{bucket_name}/**/*.zip"
            
        cmd = ["gsutil", "ls", pattern]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return []
            
        files = result.stdout.strip().split('\n')
        return [f for f in files if f]
        
    except Exception as e:
        print(f"Error listing forecasts: {e}")
        return []


def cleanup_old_forecasts(bucket_name=GCS_BUCKET, days_to_keep=7):
    """
    Remove forecasts older than specified days.
    
    Args:
        bucket_name: GCS bucket name
        days_to_keep: Number of days to retain
        
    Returns:
        Number of objects deleted
    """
    cutoff = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
    deleted = 0
    
    try:
        # List all date directories
        cmd = ["gsutil", "ls", f"gs://{bucket_name}/"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return 0
            
        for line in result.stdout.strip().split('\n'):
            # Parse date from directory name: gs://bucket/YYYY-MM-DD/
            path = line.strip().rstrip('/')
            date_str = path.split('/')[-1]
            
            try:
                dir_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                if dir_date < cutoff:
                    # Delete entire directory
                    del_cmd = ["gsutil", "-m", "-q", "rm", "-r", line]
                    del_result = subprocess.run(del_cmd, capture_output=True, timeout=120)
                    if del_result.returncode == 0:
                        print(f"Deleted old forecast directory: {date_str}")
                        deleted += 1
            except ValueError:
                # Not a date directory, skip
                continue
                
    except Exception as e:
        print(f"Error during cleanup: {e}")
        
    return deleted

def upload_html_interface(bucket_name=GCS_BUCKET):
    """
    Uploads the static bucket_index.html to index.html in the bucket.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(script_dir, "bucket_index.html")
    
    if not os.path.exists(html_path):
        print(f"Warning: HTML interface not found at {html_path}")
        return False
        
    # Upload as index.html with no-cache headers to ensure updates are seen
    dest_path = "index.html"
    gs_uri = f"gs://{bucket_name}/{dest_path}"
    
    try:
         # Set cache control to 1 minute or no-cache
        cmd = ["gsutil", "-h", "Cache-Control:public, max-age=60", "cp", html_path, gs_uri]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("HTML Interface Uploaded Successfully.")
            return True
        else:
            print(f"HTML Upload Failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error uploading HTML: {e}")
        return False


# CLI for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="GCS Upload Utility")
    parser.add_argument("--upload", help="Upload a file to GCS")
    parser.add_argument("--run-label", default="test", help="Run label (pm_forecast, am_forecast, reanalysis)")
    parser.add_argument("--model", default="HRRR", help="Model name")
    parser.add_argument("--update-index", action="store_true", help="Update index.json")
    parser.add_argument("--list", action="store_true", help="List available forecasts")
    parser.add_argument("--date", help="Filter by date (YYYY-MM-DD)")
    parser.add_argument("--cleanup", type=int, help="Delete forecasts older than N days")
    
    args = parser.parse_args()
    
    if args.upload:
        url = upload_archive(args.upload, args.run_label, args.model)
        if url:
            print(f"Public URL: {url}")
    
    if args.update_index:
        update_index()
        
    if args.list:
        forecasts = list_available_forecasts(date=args.date)
        for f in forecasts:
            print(f)
            
    if args.cleanup:
        count = cleanup_old_forecasts(days_to_keep=args.cleanup)
        print(f"Deleted {count} old directories")

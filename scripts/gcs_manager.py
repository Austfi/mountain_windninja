import os
import json
import logging
import datetime
from pathlib import Path
from google.cloud import storage
import utils
import config_loader

logger = utils.setup_logging(__name__)

class GCSManager:
    def __init__(self, bucket_name=None):
        self.bucket_name = bucket_name or config_loader.GCS_BUCKET
        self.client = None
        self.bucket = None
        self._connect()

    def _connect(self):
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {e}")
            # Don't crash init, methods will fail gracefully
            
    def upload_file_from_string(self, content, dest_path, content_type=None, cache_control=None):
        """Upload raw string content to GCS (e.g., generated KML)."""
        if not self.bucket:
            logger.error("GCS bucket not initialized.")
            return False
        try:
            blob = self.bucket.blob(dest_path)
            if cache_control:
                blob.cache_control = cache_control
            if not content_type:
                content_type = 'application/octet-stream'
            blob.upload_from_string(content, content_type=content_type)
            logger.info(f"Uploaded string content to gs://{self.bucket_name}/{dest_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload string content to {dest_path}: {e}")
            return False

    def upload_file(self, local_path, dest_path, content_type=None, cache_control=None):
        """Uploads a file to GCS."""
        if not self.bucket:
            logger.error("GCS bucket not initialized.")
            return False
            
        if not os.path.exists(local_path):
            logger.error(f"Local file not found: {local_path}")
            return False
            
        try:
            blob = self.bucket.blob(dest_path)
            if cache_control:
                blob.cache_control = cache_control
            
            # Simple content type guessing if not provided
            if not content_type:
                if local_path.endswith('.json'):
                    content_type = 'application/json'
                elif local_path.endswith('.html'):
                    content_type = 'text/html'
                elif local_path.endswith('.zip'):
                    content_type = 'application/zip'
                elif local_path.endswith('.kmz'):
                    content_type = 'application/vnd.google-earth.kmz'
                elif local_path.endswith('.kml'):
                    content_type = 'application/vnd.google-earth.kml+xml'
            
            blob.upload_from_filename(local_path, content_type=content_type)
            logger.info(f"Uploaded: {local_path} -> gs://{self.bucket_name}/{dest_path}")
            return True
        except Exception as e:
            logger.error(f"Upload failed for {local_path}: {e}")
            return False

    def upload_status(self, run_label, model, status, error=None):
        """Uploads a status JSON file."""
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
        
        # Determine strict status
        # If running, success, failure.
        
        # Don't use temp file, upload from string
        try:
            if not self.bucket: return False
            blob = self.bucket.blob(dest_path)
            blob.upload_from_string(
                json.dumps(status_data, indent=2),
                content_type='application/json',
                client=self.client
            )
            # logger.info(f"Status updated: {status}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload status: {e}")
            return False

    def update_index(self):
        """Regenerates index.json and uploads it along with bucket_index.html."""
        if not self.bucket: return False

        try:
            # 1. List all ZIP files
            blobs = list(self.client.list_blobs(self.bucket, prefix="", match_glob="**/*.zip"))
            
            forecasts = []
            for blob in blobs:
                # Path structure: YYYY-MM-DD/run_label_model/filename.zip
                parts = blob.name.split('/')
                if len(parts) >= 3:
                    date_dir = parts[0]
                    run_info = parts[1]
                    filename = parts[-1]
                    
                    # Parse run info
                    if '_' in run_info:
                        # naive split might fail if label has underscores
                        # Standard format expected: label_MODEL
                        run_parts = run_info.rsplit('_', 1)
                        if len(run_parts) == 2:
                            run_label, model = run_parts
                        else:
                            run_label = run_info
                            model = "unknown"
                    else:
                        run_label = run_info
                        model = "unknown"

                    forecasts.append({
                        "date": date_dir,
                        "run_type": run_label,
                        "model": model,
                        "filename": filename,
                        "url": f"{config_loader.GCS_PUBLIC_URL_BASE}/{blob.name}"
                    })

            # 2. List recent status files (optimization: list all status files? or just last 3 days?)
            # Listing all objects in a flat bucket is cheap if count < 10k. 
            # If excessive, prefix listing by date is better.
            # Let's list all status_*.json files for now to be safe and simple.
            status_blobs = list(self.client.list_blobs(self.bucket, match_glob="**/status_*.json"))
            
            statuses = {}
            for blob in status_blobs:
                # We need the CONTENT of the status file.
                # Only fetch content for "recent" files to avoid massive API calls?
                # Or just fetch all?
                # Optimization: Check blob updated time?
                # For now, let's fetch content. Parallelize?
                # To be robust but not slow:
                # Filter by date in path: YYYY-MM-DD/status...
                try:
                    parts = blob.name.split('/')
                    file_date = datetime.datetime.strptime(parts[0], "%Y-%m-%d")
                    if (datetime.datetime.now() - file_date).days > 3:
                        continue # Skip old statuses
                        
                    data = json.loads(blob.download_as_string())
                    # Key: YYYY-MM-DD_type_model
                    key = f"{parts[0]}_{data.get('run_type')}_{data.get('model')}"
                    statuses[key] = data
                except Exception:
                    continue

            # Sort forecasts
            forecasts.sort(key=lambda x: (x["date"], x["run_type"], x["model"]), reverse=True)

            # Determine latest forecast and reanalysis KMZ URLs for KML network links
            latest_forecast_kmz = None
            latest_reanalysis_kmz = None
            # Assuming forecasts list is sorted descending by date/run_type/model
            for f in forecasts:
                if f["run_type"].lower() == "forecast" and not latest_forecast_kmz:
                    latest_forecast_kmz = f["url"]
                if f["run_type"].lower() == "reanalysis" and not latest_reanalysis_kmz:
                    latest_reanalysis_kmz = f["url"]
                if latest_forecast_kmz and latest_reanalysis_kmz:
                    break
            kml_network_links = {
                "latest_forecast_kmz": latest_forecast_kmz,
                "latest_reanalysis_kmz": latest_reanalysis_kmz
            }

            index_data = {
                "updated": datetime.datetime.utcnow().isoformat() + "Z",
                "bucket": self.bucket_name,
                "base_url": config_loader.GCS_PUBLIC_URL_BASE,
                "description_header": "This is an automatically generated index of available forecasts.",
                "forecasts": forecasts,
                "statuses": statuses,
                "kml_network_links": kml_network_links
            }

            # Upload index.json
            index_blob = self.bucket.blob("index.json")
            index_blob.cache_control = "public, max-age=60"
            index_blob.upload_from_string(
                json.dumps(index_data, indent=2),
                content_type='application/json'
            )
            
            # Upload HTML interface
            html_path = config_loader.SCRIPTS_DIR / "bucket_index.html"
            if html_path.exists():
                self.upload_file(str(html_path), "index.html", content_type="text/html", cache_control="public, max-age=60")
            
            # Generate and upload KML network link file for Google Earth
            kml_content = self._generate_kml_network_link(latest_forecast_kmz, latest_reanalysis_kmz)
            self.upload_file_from_string(
                kml_content, 
                "latest.kml", 
                content_type="application/vnd.google-earth.kml+xml",
                cache_control="public, max-age=300"
            )
            
            logger.info("Index and KML network link updated successfully.")
            return True

        except Exception as e:
            logger.error(f"Error updating index: {e}")
            return False

    def _generate_kml_network_link(self, forecast_url, reanalysis_url):
        """Generate a KML file with network links to the latest forecast and reanalysis KMLs.
        
        This allows users to add one file to Google Earth that auto-refreshes.
        """
        base_url = config_loader.GCS_PUBLIC_URL_BASE
        
        # Always use the stable "latest" URLs for the network links
        # This ensures the KML doesn't need to change, and points to the latest uploaded run.
        forecast_href = f"{base_url}/latest/HRRR_Forecast.kml"
        reanalysis_href = f"{base_url}/latest/HRRR_Reanalysis.kml"
        
        kml = f'''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Keystone WindNinja Forecasts</name>
    <description>
      Automatically updating links to the latest WindNinja wind forecasts and reanalysis.
      Add this file to Google Earth for live updates.
    </description>
    <NetworkLink>
      <name>Latest HRRR Forecast</name>
      <description>Most recent HRRR wind forecast</description>
      <Link>
        <href>{forecast_href}</href>
        <refreshMode>onInterval</refreshMode>
        <refreshInterval>3600</refreshInterval>
      </Link>
    </NetworkLink>
    <NetworkLink>
      <name>Latest HRRR Reanalysis</name>
      <description>Most recent HRRR reanalysis (historical verification)</description>
      <Link>
        <href>{reanalysis_href}</href>
        <refreshMode>onInterval</refreshMode>
        <refreshInterval>3600</refreshInterval>
      </Link>
    </NetworkLink>
  </Document>
</kml>'''
        return kml

    def cleanup_old_forecasts(self, days_to_keep=7):
        """Deletes folder prefixes older than N days."""
        if not self.bucket: return 0
        
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
        deleted_count = 0
        
        # List top-level 'directories' (prefixes)
        # Using delimiter in list_blobs is the standard way to emulate directory listing
        iterator = self.client.list_blobs(self.bucket, delimiter='/')
        list(iterator) # populate prefixes
        
        for prefix in iterator.prefixes:
            # prefix is like "2023-10-25/"
            date_str = prefix.strip('/')
            try:
                dir_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                if dir_date < cutoff:
                    logger.info(f"Deleting old GCS prefix: {prefix}")
                    # Delete all blobs with this prefix
                    blobs_to_delete = list(self.client.list_blobs(self.bucket, prefix=prefix))
                    # Batch delete if possible, or individual
                    for blob in blobs_to_delete:
                        blob.delete()
                    deleted_count += 1
            except ValueError:
                continue
                
        return deleted_count

# Singleton instance for easy import
manager = GCSManager()

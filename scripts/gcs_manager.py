"""Google Cloud Storage upload manager.

Handles uploading forecast archives, status files, KMZ outputs,
and an HTML index page to a GCS bucket.
"""
from __future__ import annotations

import datetime
import json
import os
from pathlib import Path
import re

import config_loader
import utils

logger = utils.setup_logging(__name__)
ARCHIVE_FORECAST_RE = re.compile(
    r"^(?P<domain>.+)_(?P<run_type>forecast_\d+h|reanalysis_\d+h)_(?P<model>[^_]+)_(?P<date>\d{8})$"
)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_archive_metadata(blob_name: str):
    parts = blob_name.split("/")
    filename = parts[-1]
    stem = Path(filename).stem

    if len(parts) >= 3 and parts[0] == "archives":
        date_dir = parts[1]
        match = ARCHIVE_FORECAST_RE.match(stem)
        if match:
            return {
                "date": match.group("date") or date_dir,
                "run_type": match.group("run_type"),
                "model": match.group("model"),
                "filename": filename,
            }
        return {
            "date": date_dir,
            "run_type": stem,
            "model": "unknown",
            "filename": filename,
        }

    if len(parts) >= 3:
        date_dir, run_info = parts[0], parts[1]
        run_parts = run_info.rsplit("_", 1)
        return {
            "date": date_dir,
            "run_type": run_parts[0] if len(run_parts) == 2 else run_info,
            "model": run_parts[1] if len(run_parts) == 2 else "unknown",
            "filename": filename,
        }

    return None


class GCSManager:
    def __init__(self, bucket_name=None):
        self.bucket_name = bucket_name or config_loader.GCS_BUCKET
        self.client = None
        self.bucket = None

    def _ensure_connected(self):
        """Lazy-init: only connect to GCS when an upload is actually needed."""
        if self.bucket is not None:
            return True
        if not config_loader.GCS_UPLOAD_ENABLED:
            return False
        try:
            from google.cloud import storage
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {e}")
            return False

    def upload_file(self, local_path, dest_path, content_type=None,
                    cache_control=None):
        local_path = os.fspath(local_path)
        if not self._ensure_connected():
            return False
        if not os.path.exists(local_path):
            logger.error(f"File not found: {local_path}")
            return False
        try:
            blob = self.bucket.blob(dest_path)
            if cache_control:
                blob.cache_control = cache_control
            if not content_type:
                ext_map = {
                    ".json": "application/json",
                    ".html": "text/html",
                    ".zip": "application/zip",
                    ".kmz": "application/vnd.google-earth.kmz",
                    ".kml": "application/vnd.google-earth.kml+xml",
                }
                content_type = ext_map.get(Path(local_path).suffix)
            blob.upload_from_filename(local_path, content_type=content_type)
            logger.info(f"Uploaded: {local_path} -> gs://{self.bucket_name}/{dest_path}")
            return True
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False

    def upload_status(self, run_label, model, status, error=None):
        if not self._ensure_connected():
            return False
        today = _utcnow().strftime("%Y-%m-%d")
        dest = f"{today}/status_{run_label}_{model}.json"
        data = {
            "status": status,
            "updated": _utcnow().isoformat(),
            "run_type": run_label,
            "model": model,
            "error": error,
        }
        try:
            blob = self.bucket.blob(dest)
            blob.upload_from_string(json.dumps(data, indent=2),
                                    content_type="application/json")
            return True
        except Exception as e:
            logger.error(f"Status upload failed: {e}")
            return False

    def update_index(self):
        """Build index.json from bucket contents and upload with HTML page."""
        if not self._ensure_connected():
            return False
        try:
            blobs = list(self.client.list_blobs(self.bucket, match_glob="**/*.zip"))
            forecasts = []
            for blob in blobs:
                metadata = _parse_archive_metadata(blob.name)
                if metadata:
                    forecasts.append({
                        "date": metadata["date"],
                        "run_type": metadata["run_type"],
                        "model": metadata["model"],
                        "filename": metadata["filename"],
                        "url": f"{config_loader.GCS_PUBLIC_URL_BASE}/{blob.name}",
                    })

            forecasts.sort(key=lambda x: (x["date"], x["run_type"]), reverse=True)

            latest_kmz = f"{config_loader.GCS_PUBLIC_URL_BASE}/latest_forecast.kmz"

            index_data = {
                "updated": _utcnow().isoformat(),
                "bucket": self.bucket_name,
                "base_url": config_loader.GCS_PUBLIC_URL_BASE,
                "forecasts": forecasts,
                "kml_network_links": {"latest_forecast_kmz": latest_kmz},
            }

            idx_blob = self.bucket.blob("index.json")
            idx_blob.cache_control = "public, max-age=60"
            idx_blob.upload_from_string(json.dumps(index_data, indent=2),
                                        content_type="application/json")

            html_path = config_loader.SCRIPTS_DIR / "bucket_index.html"
            if html_path.exists():
                self.upload_file(str(html_path), "index.html",
                                 content_type="text/html",
                                 cache_control="public, max-age=60")

            kml = self._kml_network_link(latest_kmz)
            for name in ("HRRR_Forecast.kml", "latest.kml"):
                blob = self.bucket.blob(name)
                blob.cache_control = "public, max-age=300"
                blob.upload_from_string(
                    kml, content_type="application/vnd.google-earth.kml+xml")

            logger.info("Index updated.")
            return True
        except Exception as e:
            logger.error(f"Index update failed: {e}")
            return False

    def _kml_network_link(self, kmz_url):
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Mountain WindNinja Forecast</name>
    <NetworkLink>
      <name>Latest HRRR Forecast</name>
      <Link>
        <href>{kmz_url}</href>
        <refreshMode>onInterval</refreshMode>
        <refreshInterval>300</refreshInterval>
      </Link>
    </NetworkLink>
  </Document>
</kml>'''

    def cleanup_old_forecasts(self, days_to_keep=7):
        if not self._ensure_connected():
            return 0
        cutoff = _utcnow() - datetime.timedelta(days=days_to_keep)
        deleted = 0
        prefixes = set()
        for blob in self.client.list_blobs(self.bucket, prefix="archives/"):
            parts = blob.name.split("/")
            if len(parts) >= 2:
                prefixes.add("/".join(parts[:2]) + "/")
        for prefix in prefixes:
            date_str = prefix.strip("/").split("/")[-1]
            try:
                ts = datetime.datetime.strptime(date_str, "%Y%m%d").replace(
                    tzinfo=datetime.timezone.utc
                )
            except ValueError:
                continue
            if ts < cutoff:
                logger.info(f"Deleting old archive: {prefix}")
                for blob in self.client.list_blobs(self.bucket, prefix=prefix):
                    blob.delete()
                deleted += 1
        return deleted


manager = GCSManager()

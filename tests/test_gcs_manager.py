from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import gcs_manager


def test_update_index_uploads_json_and_html(tmp_path, monkeypatch):
    bucket_index = tmp_path / "bucket_index.html"
    bucket_index.write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(gcs_manager.config_loader, "SCRIPTS_DIR", tmp_path)
    monkeypatch.setattr(gcs_manager.config_loader, "GCS_PUBLIC_URL_BASE",
                        "https://storage.googleapis.com/test-bucket")

    mgr = gcs_manager.GCSManager(bucket_name="test-bucket")
    mgr.bucket = MagicMock()
    mgr.client = MagicMock()

    mock_blob = MagicMock()
    mock_blob.name = "archives/20260101/my_area_forecast_18h_HRRR_20260101.zip"
    mgr.client.list_blobs.return_value = [mock_blob]

    created_blobs = {}

    def blob_factory(name):
        blob = MagicMock()
        blob.name = name
        created_blobs[name] = blob
        return blob

    mgr.bucket.blob.side_effect = blob_factory

    with patch.object(mgr, "upload_file", return_value=True) as upload_mock:
        assert mgr.update_index() is True

    assert "index.json" in created_blobs
    index_content = json.loads(
        created_blobs["index.json"].upload_from_string.call_args.args[0]
    )
    assert len(index_content["forecasts"]) == 1
    assert index_content["forecasts"][0]["date"] == "20260101"
    assert index_content["forecasts"][0]["run_type"] == "forecast_18h"
    assert index_content["forecasts"][0]["model"] == "HRRR"
    assert index_content["forecasts"][0]["url"].endswith(
        "my_area_forecast_18h_HRRR_20260101.zip"
    )

    assert "HRRR_Forecast.kml" in created_blobs
    assert "latest.kml" in created_blobs

    upload_mock.assert_called_once_with(
        str(bucket_index), "index.html",
        content_type="text/html",
        cache_control="public, max-age=60",
    )


def test_upload_file_returns_false_when_upload_disabled(monkeypatch):
    monkeypatch.setattr(gcs_manager.config_loader, "GCS_UPLOAD_ENABLED", False)
    mgr = gcs_manager.GCSManager(bucket_name="test")
    assert mgr.upload_file("/tmp/nope.txt", "dest.txt") is False


def test_upload_status_returns_false_when_upload_disabled(monkeypatch):
    monkeypatch.setattr(gcs_manager.config_loader, "GCS_UPLOAD_ENABLED", False)
    mgr = gcs_manager.GCSManager(bucket_name="test")
    assert mgr.upload_status("forecast", "HRRR", "running") is False


def test_cleanup_old_forecasts_deletes_only_old_archive_prefixes():
    mgr = gcs_manager.GCSManager(bucket_name="test-bucket")
    mgr.bucket = MagicMock()
    mgr.client = MagicMock()

    old_prefix_blob = MagicMock()
    old_prefix_blob.name = "archives/20000101/archive.zip"
    new_prefix_blob = MagicMock()
    new_prefix_blob.name = "archives/29990101/archive.zip"

    old_archive_blob = MagicMock()
    new_archive_blob = MagicMock()

    def list_blobs(_bucket, prefix=None, match_glob=None):
        if prefix == "archives/":
            return [old_prefix_blob, new_prefix_blob]
        if prefix == "archives/20000101/":
            return [old_archive_blob]
        if prefix == "archives/29990101/":
            return [new_archive_blob]
        return []

    mgr.client.list_blobs.side_effect = list_blobs

    deleted = mgr.cleanup_old_forecasts(days_to_keep=7)

    assert deleted == 1
    old_archive_blob.delete.assert_called_once()
    new_archive_blob.delete.assert_not_called()

import pytest
from unittest.mock import MagicMock, patch
import json
import gcs_manager
import config_loader

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

@pytest.fixture
def manager(mock_storage_client):
    # Re-init manager to pick up mocked client
    return gcs_manager.GCSManager(bucket_name="test-bucket")

def test_upload_file_success(manager):
    manager.bucket = MagicMock()
    blob = MagicMock()
    manager.bucket.blob.return_value = blob
    
    # Create dummy file
    with patch('os.path.exists', return_value=True):
        result = manager.upload_file("dummy.txt", "dest/dummy.txt")
        
    assert result is True
    manager.bucket.blob.assert_called_with("dest/dummy.txt")
    blob.upload_from_filename.assert_called_with("dummy.txt", content_type=None)

def test_upload_status_success(manager):
    manager.bucket = MagicMock()
    blob = MagicMock()
    manager.bucket.blob.return_value = blob
    
    result = manager.upload_status("forecast", "HRRR", "success")
    
    assert result is True
    blob.upload_from_string.assert_called_once()
    args, kwargs = blob.upload_from_string.call_args
    data = json.loads(args[0])
    assert data["status"] == "success"
    assert data["model"] == "HRRR"

def test_update_index_structure(manager):
    manager.bucket = MagicMock()
    
    # Use today's date so filter doesn't skip it
    import datetime
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # Mock list_blobs for Zips
    zip_blob = MagicMock()
    zip_blob.name = f"{today}/forecast_HRRR/archive.zip"
    
    # Mock list_blobs for Status
    status_blob = MagicMock()
    status_blob.name = f"{today}/status_forecast_HRRR.json"
    status_blob.download_as_string.return_value = json.dumps({
        "status":"success", "run_type":"forecast", "model":"HRRR"
    })
    
    manager.client.list_blobs.side_effect = [[zip_blob], [status_blob]]
    
    # Mock blob for index.json
    index_blob = MagicMock()
    manager.bucket.blob.return_value = index_blob
    
    with patch('config_loader.SCRIPTS_DIR') as mock_scripts_dir:
        # Mock HTML existence check
        mock_scripts_dir.__truediv__.return_value.exists.return_value = False
        
        result = manager.update_index()
    
    assert result is True
    # Verify index structure - now we have 2 calls: index.json and latest.kml
    assert index_blob.upload_from_string.call_count == 2
    
    # First call is index.json
    first_call_args, first_call_kwargs = index_blob.upload_from_string.call_args_list[0]
    index_json = json.loads(first_call_args[0])
    
    assert len(index_json["forecasts"]) == 1
    assert index_json["forecasts"][0]["filename"] == "archive.zip"
    assert f"{today}_forecast_HRRR" in index_json["statuses"]
    assert "kml_network_links" in index_json
    
    # Second call is latest.kml
    second_call_args, second_call_kwargs = index_blob.upload_from_string.call_args_list[1]
    assert "application/vnd.google-earth.kml+xml" in str(second_call_kwargs)
    assert "<kml" in second_call_args[0]

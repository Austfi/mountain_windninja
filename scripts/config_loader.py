import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Base Paths
SCRIPTS_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPTS_DIR.parent
CONFIG_DIR = BASE_DIR / "config"
ARCHIVE_DIR = BASE_DIR / "archives"
TEMP_DIR = BASE_DIR / "temp"

# WindNinja Configuration
WINDNINJA_CLI = os.getenv("WINDNINJA_CLI", "/home/austin_finnell/windninja_build/src/cli/WindNinja_cli")

# GCS Configuration
GCS_BUCKET = os.getenv("GCS_BUCKET", "wrf-austin-bucket")
GCS_UPLOAD_ENABLED = os.getenv("GCS_UPLOAD_ENABLED", "true").lower() == "true"
GCS_PUBLIC_URL_BASE = f"https://storage.googleapis.com/{GCS_BUCKET}"

# Domain Configuration
# Maps domain names to config template files
DOMAIN_TEMPLATES = {
    "small": CONFIG_DIR / "keystone_template.cfg",
    "large": CONFIG_DIR / "keystone_template_large.cfg"
}

# Ensure critical directories exist
def init_directories():
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

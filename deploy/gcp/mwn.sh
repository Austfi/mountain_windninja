#!/usr/bin/env bash
# Mountain WindNinja -- operator CLI
# Run ./deploy/gcp/mwn.sh help for usage.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_DIR"

# ── Docker helpers ─────────────────────────────────────────────────────────

DOCKER_PREFIX=()

pick_docker() {
  if docker info >/dev/null 2>&1; then
    DOCKER_PREFIX=(docker)
  elif command -v sudo >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
    DOCKER_PREFIX=(sudo docker)
  else
    echo "Docker is not running. Install it first with ./deploy/gcp/bootstrap_repo.sh"
    exit 1
  fi
}

compose() {
  if "${DOCKER_PREFIX[@]}" compose version >/dev/null 2>&1; then
    "${DOCKER_PREFIX[@]}" compose --profile tools "$@"
  else
    echo "Docker Compose not found. Run ./deploy/gcp/bootstrap_repo.sh"
    exit 1
  fi
}

# ── Commands ───────────────────────────────────────────────────────────────

cmd_help() {
  cat <<'EOF'
Mountain WindNinja -- operator commands

Usage: ./deploy/gcp/mwn.sh <command> [options]

Commands:
  build                Build the Docker image (~30 min first time)
  check                Run preflight checks (WindNinja, DEM/LCP, dirs)
  run [flags]          Run a WindNinja simulation inside the container
  shell                Open a bash shell inside the container
  fetch-dem [flags]    Download DEM (source: us, srtm, gmted) into static_data/
  fetch-lcp [flags]    Download LCP from LANDFIRE into static_data/ (US only)
  lcp-build <tif>      Convert a LANDFIRE GeoTIFF to LCP format
  clean                Clear cached mesh and temp files (fixes most errors)
  upload               Upload latest results to GCS bucket
  schedule             Start the automatic hourly scheduler
  stop                 Stop the scheduler
  logs                 View scheduler logs
  update               Pull latest code and rebuild

Run flags (passed to daily_run.py):
  --mode forecast|reanalysis|domain-average
                                 Run mode (default: forecast)
  --model HRRR|NBM|NAM|RAP|GFS  Weather model (default: HRRR)
  --hours N                      Forecast window (default: 18)
  --domain <name>                Domain from domains.json
  --speed N                      Wind speed for domain-average mode
  --direction N                  Wind direction (degrees) for domain-average
  --speed-units mph|mps|kph|kts  Units for --speed (default: mph)
  --height N                     Output wind height in meters (default: 10)
                                   2 = felt wind, 10 = standard tower height
  --keep-temp                    Keep output files (don't archive)
  --no-upload                    Skip GCS upload
  --dry-run                      Generate config only, don't run WindNinja

Weather models:
  HRRR       3 km CONUS, hourly updates, up to 18h forecast (default)
  NBM        2.5 km CONUS, calibrated blend, most accurate
  NAM        3 km CONUS (nested), longer range
  NAM-CONUS  12 km CONUS
  NAM-ALASKA 11.25 km Alaska
  RAP        13 km CONUS, rapid refresh
  GFS        0.25 deg global, long-range (up to 16 days)

Examples:
  ./deploy/gcp/mwn.sh build
  ./deploy/gcp/mwn.sh run --hours 6 --model HRRR
  ./deploy/gcp/mwn.sh run --mode reanalysis --hours 12
  ./deploy/gcp/mwn.sh run --mode domain-average --speed 20 --direction 270
  ./deploy/gcp/mwn.sh run --hours 6 --height 2        # 2m felt wind
  ./deploy/gcp/mwn.sh run --model GFS --hours 48
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10
  ./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/my_area.lcp
  ./deploy/gcp/mwn.sh lcp-build static_data/landscape.tif static_data/my_area.lcp
  ./deploy/gcp/mwn.sh shell
EOF
}

cmd_build() {
  pick_docker
  compose build
  echo "Build complete. Run: ./deploy/gcp/mwn.sh check"
}

cmd_check() {
  pick_docker
  compose run --rm shell python ./scripts/preflight_check.py "$@"
}

cmd_clean() {
  echo "Clearing cached mesh data..."
  sudo rm -rf static_data/NINJAFOAM_* 2>/dev/null || true
  echo "Clearing temp run output..."
  sudo rm -rf runtime/temp/* 2>/dev/null || true
  echo "Clean. Ready to run."
}

cmd_run() {
  pick_docker
  echo "Running preflight check..."
  if ! compose run --rm shell python ./scripts/preflight_check.py 2>&1; then
    echo ""
    echo "Preflight check failed. Fix the issues above, then retry."
    echo "Run ./deploy/gcp/mwn.sh check for details."
    exit 1
  fi
  if ! compose run --rm shell bash -c \
    "source /opt/openfoam9/etc/bashrc 2>/dev/null || true; \
     export FOAM_USER_LIBBIN=/usr/local/lib/; \
     cd /opt/mountain_windninja/runtime; \
     /opt/venv/bin/python /opt/mountain_windninja/scripts/daily_run.py $*"; then
    echo ""
    echo "Run failed. Cleaning corrupted mesh cache..."
    sudo rm -rf static_data/NINJAFOAM_* 2>/dev/null || true
    sudo rm -rf runtime/temp/* 2>/dev/null || true
    echo "Cache cleared. Fix the issue and re-run."
    exit 1
  fi
}

cmd_shell() {
  pick_docker
  compose run --rm shell /bin/bash
}

cmd_fetch_dem() {
  local north="${1:-}"
  local east="${2:-}"
  local south="${3:-}"
  local west="${4:-}"
  local output="${5:-static_data/dem_download.tif}"
  local src="${6:-us}"
  local resolution="${7:-}"

  if [ -z "$north" ] || [ -z "$east" ] || [ -z "$south" ] || [ -z "$west" ]; then
    cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh fetch-dem <north> <east> <south> <west> [output] [source] [resolution]

Downloads elevation data (DEM) using WindNinja's built-in fetch_dem tool.

Arguments:
  north/east/south/west  Bounding box in decimal degrees (latitude/longitude)
  output                 Output file path (default: static_data/dem_download.tif)
  source                 Data source (default: us). Options:
                           us    - USGS 3DEP 10m (US only, no key needed, true 10m)
                           srtm  - SRTM 30m via OpenTopography (global, needs API key)
                           gmted - GMTED2010 (~250m global, no key needed)
  resolution             Output resolution in meters (default: 10 for us, 30 for srtm)

Note: srtm source requires an OpenTopography API key. Set CUSTOM_SRTM_API_KEY
in config/runtime.env. Get a free key at https://opentopography.org/

Examples:
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif srtm 30
  ./deploy/gcp/mwn.sh fetch-dem 45.5 7.0 45.0 6.5 static_data/alps.tif srtm 30
USAGE
    exit 1
  fi

  pick_docker

  if [ "$src" = "us" ]; then
    local res="${resolution:-10}"
    local vrt="/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"

    # Auto-detect UTM zone from center of bounding box
    local center_lon
    center_lon=$(echo "($east + ($west)) / 2" | bc -l)
    local center_lat
    center_lat=$(echo "($north + $south) / 2" | bc -l)
    local utm_zone
    utm_zone=$(echo "($center_lon + 180) / 6 + 1" | bc | cut -d. -f1)
    local epsg
    if (( $(echo "$center_lat >= 0" | bc -l) )); then
      epsg=$((32600 + utm_zone))
    else
      epsg=$((32700 + utm_zone))
    fi

    echo "Downloading DEM: north=$north east=$east south=$south west=$west"
    echo "Source: USGS 3DEP 1/3 arc-second | Output: $output | Resolution: ${res}m | CRS: EPSG:${epsg}"
    compose run --rm shell gdalwarp \
      -te "$west" "$south" "$east" "$north" \
      -te_srs EPSG:4326 \
      -t_srs "EPSG:${epsg}" \
      -tr "$res" "$res" \
      -r bilinear \
      -overwrite \
      "$vrt" "$output"
  elif [ "$src" = "srtm" ] || [ "$src" = "gmted" ]; then
    local res="${resolution:-30}"
    echo "Downloading DEM: north=$north east=$east south=$south west=$west"
    echo "Source: $src | Output: $output | Resolution: ${res}m"
    compose run --rm shell fetch_dem \
      --bbox "$north" "$east" "$south" "$west" \
      --src "$src" \
      --out_res "$res" \
      "$output"
  else
    echo "Unknown source: $src (use: us, srtm, or gmted)"
    exit 1
  fi

  echo ""
  echo "DEM saved to $output"
  echo "Next: add it to config/domains.json and run ./deploy/gcp/mwn.sh check"
}

cmd_fetch_lcp() {
  local north="${1:-}"
  local east="${2:-}"
  local south="${3:-}"
  local west="${4:-}"
  local output="${5:-static_data/lcp_download.lcp}"

  if [ -z "$north" ] || [ -z "$east" ] || [ -z "$south" ] || [ -z "$west" ]; then
    cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh fetch-lcp <north> <east> <south> <west> [output]

Downloads an LCP (Landscape) file from LANDFIRE. LCP files include elevation
plus vegetation/fuel data (8 bands), giving WindNinja real vegetation info
instead of a uniform assumption.

Arguments:
  north/east/south/west  Bounding box in decimal degrees (latitude/longitude)
  output                 Output file path (default: static_data/lcp_download.lcp)

Note: LANDFIRE downloads can take several minutes due to server processing.
Only available for the United States.

Examples:
  ./deploy/gcp/mwn.sh fetch-lcp 40.0 -105.0 39.5 -105.5
  ./deploy/gcp/mwn.sh fetch-lcp 40.0 -105.0 39.5 -105.5 static_data/my_area.lcp
USAGE
    exit 1
  fi

  pick_docker
  echo "Downloading LCP from LANDFIRE: north=$north east=$east south=$south west=$west"
  echo "Output: $output (this may take several minutes...)"
  compose run --rm shell fetch_dem \
    --bbox "$north" "$east" "$south" "$west" \
    --src lcp \
    "$output"

  # Generate .prj sidecar (required by WindNinja for LCP files)
  local prj_path="${output%.lcp}.prj"
  echo "Generating projection file: $prj_path"
  compose run --rm shell bash -c "gdalsrsinfo -o wkt '$output' > '$prj_path'"

  echo ""
  echo "LCP saved to $output"
  echo "Next: add it to config/domains.json and run ./deploy/gcp/mwn.sh check"
}

cmd_lcp_build() {
  local input="${1:-}"
  local output="${2:-static_data/summit_county_surface.lcp}"
  if [ -z "$input" ]; then
    echo "Usage: ./deploy/gcp/mwn.sh lcp-build <input.tif> [output.lcp]"
    exit 1
  fi
  pick_docker
  compose run --rm shell python ./scripts/build_lcp_from_geotiff.py "$input" "$output"
}

cmd_upload() {
  pick_docker
  compose run --rm shell python -c "
import sys; sys.path.insert(0, 'scripts')
from gcs_manager import manager
manager.update_index()
print('Upload complete.')
"
}

cmd_schedule() {
  pick_docker
  compose up -d scheduler
  echo "Scheduler started. Forecasts will run at :15 past every hour."
  echo "View logs: ./deploy/gcp/mwn.sh logs"
}

cmd_stop() {
  pick_docker
  compose stop scheduler 2>/dev/null || true
  echo "Scheduler stopped."
}

cmd_logs() {
  pick_docker
  compose logs -f scheduler
}

cmd_update() {
  git pull --ff-only
  pick_docker
  compose build
  compose up -d scheduler 2>/dev/null || true
  echo "Updated and rebuilt."
}

# ── Dispatch ───────────────────────────────────────────────────────────────

COMMAND="${1:-help}"
shift 2>/dev/null || true

case "$COMMAND" in
  help|-h|--help)  cmd_help ;;
  build)           cmd_build ;;
  check)           cmd_check "$@" ;;
  run)             cmd_run "$@" ;;
  shell)           cmd_shell ;;
  clean)           cmd_clean ;;
  fetch-dem)       cmd_fetch_dem "$@" ;;
  fetch-lcp)       cmd_fetch_lcp "$@" ;;
  lcp-build)       cmd_lcp_build "$@" ;;
  upload)          cmd_upload ;;
  schedule)        cmd_schedule ;;
  stop)            cmd_stop ;;
  logs)            cmd_logs ;;
  update)          cmd_update ;;
  *)
    echo "Unknown command: $COMMAND"
    echo "Run ./deploy/gcp/mwn.sh help for usage."
    exit 1
    ;;
esac

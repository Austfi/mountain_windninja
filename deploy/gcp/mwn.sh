#!/usr/bin/env bash
# Mountain WindNinja -- operator CLI
# Run ./deploy/gcp/mwn.sh help for usage.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_DIR"

HOST_PYTHON="${PYTHON:-python3}"
DEFAULT_REMOTE_IMAGE="ghcr.io/austfi/mountain-windninja:3.12.2-herbie.2"

if [ -f "$REPO_DIR/config/runtime.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_DIR/config/runtime.env"
  set +a
fi

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

host_python() {
  if ! command -v "$HOST_PYTHON" >/dev/null 2>&1; then
    echo "python3 is required on the host for this command."
    exit 1
  fi
  "$HOST_PYTHON" "$@"
}

extract_run_domain() {
  local domain=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --domain)
        shift
        domain="${1:-}"
        ;;
      --domain=*)
        domain="${1#--domain=}"
        ;;
    esac
    shift || true
  done
  printf '%s\n' "$domain"
}

validate_bbox() {
  host_python ./scripts/domain_registry.py validate-bbox "$1" "$2" "$3" "$4"
}

resolve_center_bbox() {
  local lat="$1"
  local lon="$2"
  local size_km="$3"
  local size_mi="$4"
  local radius_km="$5"
  local radius_mi="$6"
  local args=(./scripts/area_bounds.py center "$lat" "$lon")
  if [ -n "$size_km" ]; then
    args+=(--size-km "$size_km")
  fi
  if [ -n "$size_mi" ]; then
    args+=(--size-mi "$size_mi")
  fi
  if [ -n "$radius_km" ]; then
    args+=(--radius-km "$radius_km")
  fi
  if [ -n "$radius_mi" ]; then
    args+=(--radius-mi "$radius_mi")
  fi
  host_python "${args[@]}"
}

resolve_area_file_bbox() {
  local path="$1"
  local padding_km="$2"
  host_python ./scripts/area_bounds.py file "$path" --padding-km "$padding_km"
}

register_domain_if_requested() {
  local domain_key="$1"
  local label="$2"
  local set_default="$3"
  local terrain_path="$4"

  if [ -z "$domain_key" ]; then
    return 0
  fi

  local args=(./scripts/domain_registry.py register-domain "$domain_key" "$terrain_path")
  if [ -n "$label" ]; then
    args+=(--label "$label")
  fi
  if [ "$set_default" = "1" ]; then
    args+=(--set-default)
  fi
  host_python "${args[@]}"
}

set_runtime_env() {
  host_python ./scripts/domain_registry.py set-env "$1" "$2"
}

runtime_env_has_key() {
  local key="$1"
  [ -f "$REPO_DIR/config/runtime.env" ] && grep -q "^${key}=" "$REPO_DIR/config/runtime.env"
}

ensure_runtime_env_key() {
  local key="$1"
  local value="$2"
  if ! runtime_env_has_key "$key"; then
    set_runtime_env "$key" "$value"
  fi
}

pull_image() {
  local image="$1"
  pick_docker
  if ! "${DOCKER_PREFIX[@]}" pull "$image"; then
    return 1
  fi
  set_runtime_env MWN_DOCKER_IMAGE "$image"
  echo "Pulled $image and recorded MWN_DOCKER_IMAGE in config/runtime.env"
}

# ── Commands ───────────────────────────────────────────────────────────────

cmd_help() {
  if [ "${1:-}" = "advanced" ]; then
    cmd_help_advanced
    return
  fi

  cat <<'EOF'
Mountain WindNinja

Usage: ./deploy/gcp/mwn.sh <command> [options]

Beginner path:
  1. init
  2. fetch-terrain --center LAT LON --size-km N --domain KEY --label "Area Name"
  3. check
  4. smoke
  5. run --hours 6

Beginner commands:
  init                 Create local dirs/config without overwriting config/runtime.env
  fetch-terrain        Download/register DEM and LCP terrain for your area
  check [--domain KEY] Run preflight checks
  smoke [--domain KEY] Run a deterministic 1-hour test
  run [flags]          Run a forecast or reanalysis
  clean                Clear cached mesh and temp output

Common run flags:
  --hours N            Forecast window
  --domain KEY         Use a registered domain
  --weather-source native|herbie
                      Use WindNinja native weather downloads or Herbie
  --keep-temp          Keep raw output in runtime/temp/
  --mode reanalysis    Run historical HRRR reanalysis

Examples:
  ./deploy/gcp/mwn.sh init
  ./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 --domain my_area --label "My Area"
  ./deploy/gcp/mwn.sh check
  ./deploy/gcp/mwn.sh smoke
  ./deploy/gcp/mwn.sh run --hours 6

More:
  ./deploy/gcp/mwn.sh help advanced
  docs/quickstart.md
EOF
}

cmd_help_advanced() {
  cat <<'EOF'
Mountain WindNinja -- advanced commands

Usage: ./deploy/gcp/mwn.sh <command> [options]

Setup and images:
  build                Alias for build-local
  build-local          Build the Docker image locally
  pull [IMAGE]         Pull a published Docker image and record it in runtime.env
  update               Pull latest code and rebuild

Terrain:
  domain create KEY    Terrain download + registration wrapper
  fetch-terrain        Download DEM and LCP for one domain
  fetch-dem [flags]    Download DEM (source: us, srtm, gmted)
  fetch-lcp [flags]    Download LCP from LANDFIRE
  lcp-build <tif>      Convert LANDFIRE GeoTIFF to LCP

Runtime:
  shell                Open a bash shell inside the container
  run-grid             Run one timestep from speed/direction .asc grids
  forcing-from-grib    Convert one GRIB/NetCDF wind timestep to .asc grids
  upload               Upload latest results to GCS bucket
  schedule             Start the automatic scheduler
  stop                 Stop the scheduler
  logs                 View scheduler logs

Validation:
  synoptic-points      Build WindNinja point CSV from Synoptic station metadata
  validate             Compare WindNinja point output against Synoptic observations
  validate-rasters     Compare nearest WindNinja/HRRR rasters against Synoptic
  validate-study       Run a chunked Synoptic/HRRR/WindNinja validation study
  validate-k0co-height-hrrr
                       Test K0CO raw vs height-adjusted HRRR grid forcing
  plot-validation      Build static SVG/HTML plots from validation samples

Run flags:
  --mode forecast|reanalysis|domain-average
  --model HRRR|NBM|NAM|NAM-CONUS|NAM-ALASKA|RAP|GFS
          Herbie supports HRRR, GFS, RRFS, and HRRRAK
  --weather-source native|herbie
  --herbie-product PRODUCT
  --herbie-member MEMBER
  --herbie-domain DOMAIN
  --herbie-cycle UTC
  --herbie-priority aws,google,azure,nomads
  --herbie-extra KEY=VALUE
  --hours N
  --start UTC
  --end UTC
  --domain KEY
  --speed N
  --direction N
  --speed-units mph|mps|kph|kts
  --height N
  --points-file PATH
  --points-output PATH
  --keep-temp
  --no-upload
  --dry-run

Grid forcing:
  run-grid --speed-grid speed.asc --direction-grid direction.asc --time UTC --domain KEY
  forcing-from-grib INPUT --domain KEY --time UTC --u-var UGRD --v-var VGRD --level 10m --out DIR
  forcing-from-grib INPUT --domain KEY --time UTC --speed-var WIND --direction-var WDIR --level 10m --out DIR

Docs:
  docs/commands.md
  docs/terrain.md
  docs/scheduling.md
  docs/validation.md
EOF
}

cmd_init() {
  local image_mode="pull"
  local force_image="0"
  local created_runtime_env="0"

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --image)
        shift
        image_mode="${1:-}"
        ;;
      --image=*)
        image_mode="${1#--image=}"
        ;;
      --force-image)
        force_image="1"
        ;;
      -h|--help)
        cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh init [--image pull|local|skip] [--force-image]

Creates runtime/, static_data/, and config/runtime.env when missing.
Existing config/runtime.env values are preserved unless --force-image is used.

Image modes:
  pull   Pull the published GHCR image for new configs (default)
  local  Use the local image name and build with build-local when ready
  skip   Do not change or pull an image

Options:
  --force-image  Allow init to replace MWN_DOCKER_IMAGE in an existing runtime.env
USAGE
        exit 0
        ;;
      *)
        echo "Unknown init option: $1"
        echo "Usage: ./deploy/gcp/mwn.sh init [--image pull|local|skip] [--force-image]"
        exit 1
        ;;
    esac
    shift || true
  done

  case "$image_mode" in
    local|pull|skip) ;;
    *)
      echo "Unknown image mode: $image_mode (use: local, pull, or skip)"
      exit 1
      ;;
  esac

  mkdir -p "$REPO_DIR/runtime" "$REPO_DIR/static_data"
  echo "Ensured runtime/ and static_data/ exist."

  if [ ! -f "$REPO_DIR/config/runtime.env" ]; then
    cp "$REPO_DIR/config/runtime.env.example" "$REPO_DIR/config/runtime.env"
    created_runtime_env="1"
    echo "Created config/runtime.env from config/runtime.env.example."
  else
    echo "config/runtime.env already exists. Using existing config."
  fi

  local can_change_image="$force_image"
  if [ "$created_runtime_env" = "1" ] || ! runtime_env_has_key MWN_DOCKER_IMAGE; then
    can_change_image="1"
  fi

  case "$image_mode" in
    local)
      pick_docker
      if [ "$force_image" = "1" ]; then
        set_runtime_env MWN_DOCKER_IMAGE "mountain-windninja:local"
      else
        ensure_runtime_env_key MWN_DOCKER_IMAGE "mountain-windninja:local"
      fi
      echo "Image mode: local. Build it with: ./deploy/gcp/mwn.sh build-local"
      ;;
    pull)
      if [ "$can_change_image" != "1" ]; then
        echo "Image mode: pull skipped because MWN_DOCKER_IMAGE already exists."
        echo "Use --force-image to replace it with $DEFAULT_REMOTE_IMAGE."
      elif ! pull_image "$DEFAULT_REMOTE_IMAGE"; then
        echo ""
        echo "Could not pull $DEFAULT_REMOTE_IMAGE."
        if [ "$force_image" = "1" ]; then
          set_runtime_env MWN_DOCKER_IMAGE "mountain-windninja:local"
        else
          ensure_runtime_env_key MWN_DOCKER_IMAGE "mountain-windninja:local"
        fi
        echo "Fallback: ./deploy/gcp/mwn.sh build-local"
      fi
      ;;
    skip)
      echo "Image mode: skip. Leaving MWN_DOCKER_IMAGE unchanged."
      ;;
  esac

  echo "Init complete. Next: ./deploy/gcp/mwn.sh fetch-terrain --center LAT LON --size-km 10 --domain <key>"
}

cmd_build_local() {
  pick_docker
  MWN_DOCKER_IMAGE=mountain-windninja:local compose build
  set_runtime_env MWN_DOCKER_IMAGE "mountain-windninja:local"
  echo "Build complete. Recorded MWN_DOCKER_IMAGE=mountain-windninja:local in config/runtime.env"
  echo "Run: ./deploy/gcp/mwn.sh check"
}

cmd_build() {
  cmd_build_local
}

cmd_pull() {
  local image="${1:-$DEFAULT_REMOTE_IMAGE}"
  if [ "$#" -gt 1 ]; then
    echo "Usage: ./deploy/gcp/mwn.sh pull [IMAGE]"
    exit 1
  fi
  pull_image "$image"
}

cmd_check() {
  pick_docker
  compose run --rm shell python ./scripts/preflight_check.py "$@"
}

print_preflight_guidance() {
  cat <<'EOF'

Common fixes:
  Missing config/runtime.env:     ./deploy/gcp/mwn.sh init
  Missing Docker image:           ./deploy/gcp/mwn.sh pull || ./deploy/gcp/mwn.sh build-local
  Missing terrain/domain:         ./deploy/gcp/mwn.sh fetch-terrain --center LAT LON --size-km 10 --domain <key>
  Need to prove Docker first:     ./deploy/gcp/mwn.sh demo-smoke
EOF
}

print_run_failure_guidance() {
  cat <<'EOF'

Next checks if this repeats:
  1. Clear generated mesh/temp output: ./deploy/gcp/mwn.sh clean
  2. Retry the deterministic path:     ./deploy/gcp/mwn.sh smoke --keep-temp
  3. For small domains, reduce num_threads in config/template.cfg
  4. If reanalysis says GCS credentials are missing, rebuild: ./deploy/gcp/mwn.sh build-local
EOF
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
  local run_domain
  run_domain="$(extract_run_domain "$@")"
  echo "Running preflight check..."
  if [ -n "$run_domain" ]; then
    compose run --rm shell python ./scripts/preflight_check.py --domain "$run_domain" 2>&1
  else
    compose run --rm shell python ./scripts/preflight_check.py 2>&1
  fi
  local preflight_status=$?
  if [ "$preflight_status" -ne 0 ]; then
    echo ""
    echo "Preflight check failed. Fix the issues above, then retry."
    echo "Run ./deploy/gcp/mwn.sh check for details."
    print_preflight_guidance
    return 1
  fi
  if [ -n "${MWN_NUM_THREADS:-}" ]; then
    compose run --rm -e "MWN_NUM_THREADS=${MWN_NUM_THREADS}" shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       cd /opt/mountain_windninja/runtime
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/daily_run.py "$@"' \
      bash "$@"
  else
    compose run --rm shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       cd /opt/mountain_windninja/runtime
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/daily_run.py "$@"' \
      bash "$@"
  fi
  local run_status=$?
  if [ "$run_status" -ne 0 ]; then
    echo ""
    echo "Run failed. Cleaning corrupted mesh cache..."
    sudo rm -rf static_data/NINJAFOAM_* 2>/dev/null || true
    sudo rm -rf runtime/temp/* 2>/dev/null || true
    echo "Cache cleared. Fix the issue and re-run."
    print_run_failure_guidance
    return 1
  fi
}

cmd_run_grid() {
  pick_docker
  if [ -n "${MWN_NUM_THREADS:-}" ]; then
    compose run --rm -e "MWN_NUM_THREADS=${MWN_NUM_THREADS}" shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       cd /opt/mountain_windninja/runtime
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/gridded_run.py "$@"' \
      bash "$@"
  else
    compose run --rm shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       cd /opt/mountain_windninja/runtime
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/gridded_run.py "$@"' \
      bash "$@"
  fi
  local run_status=$?
  if [ "$run_status" -ne 0 ]; then
    echo ""
    echo "Grid run failed. Cleaning corrupted mesh cache..."
    sudo rm -rf static_data/NINJAFOAM_* 2>/dev/null || true
    sudo rm -rf runtime/temp/* 2>/dev/null || true
    echo "Cache cleared. Fix the issue and re-run."
    print_run_failure_guidance
    return 1
  fi
}

cmd_forcing_from_grib() {
  pick_docker
  compose run --rm shell python ./scripts/forcing_from_grib.py "$@"
}

cmd_smoke() {
  local domain=""
  local keep_temp="0"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --domain)
        shift
        domain="${1:-}"
        ;;
      --domain=*)
        domain="${1#--domain=}"
        ;;
      --keep-temp)
        keep_temp="1"
        ;;
      -h|--help)
        echo "Usage: ./deploy/gcp/mwn.sh smoke [--domain NAME] [--keep-temp]"
        exit 0
        ;;
      *)
        echo "Unknown smoke option: $1"
        echo "Usage: ./deploy/gcp/mwn.sh smoke [--domain NAME] [--keep-temp]"
        exit 1
        ;;
    esac
    shift || true
  done

  local args=(--mode domain-average --speed 10 --direction 270 --hours 1 --no-upload)
  if [ -n "$domain" ]; then
    args+=(--domain "$domain")
  fi
  if [ "$keep_temp" = "1" ]; then
    args+=(--keep-temp)
  fi
  cmd_run "${args[@]}"
}

DEMO_BACKUP_DOMAINS=""
DEMO_DOMAIN_KEY=""
DEMO_DEM_PATH=""
DEMO_KEEP_DATA="0"

demo_smoke_cleanup() {
  local status=$?
  if [ -n "$DEMO_BACKUP_DOMAINS" ] && [ -f "$DEMO_BACKUP_DOMAINS" ]; then
    cp "$DEMO_BACKUP_DOMAINS" "$REPO_DIR/config/domains.json"
    rm -f "$DEMO_BACKUP_DOMAINS"
  fi
  if [ -n "$DEMO_DOMAIN_KEY" ]; then
    rm -rf "$REPO_DIR/static_data/NINJAFOAM_${DEMO_DOMAIN_KEY}_"* 2>/dev/null || \
      sudo rm -rf "$REPO_DIR/static_data/NINJAFOAM_${DEMO_DOMAIN_KEY}_"* 2>/dev/null || true
  fi
  if [ "$DEMO_KEEP_DATA" != "1" ] && [ -n "$DEMO_DEM_PATH" ]; then
    rm -f "$DEMO_DEM_PATH" 2>/dev/null || true
  fi
  return "$status"
}

cmd_demo_smoke() {
  local keep_temp="0"
  local keep_data="0"

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --keep-temp)
        keep_temp="1"
        ;;
      --keep-data)
        keep_data="1"
        ;;
      -h|--help)
        cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh demo-smoke [--keep-temp] [--keep-data]

Creates a synthetic 10 km DEM, temporarily registers a demo domain, runs
preflight and smoke, restores config/domains.json, and removes generated mesh
cache. Use this before downloading real terrain to prove Docker + WindNinja work.

Options:
  --keep-temp   Keep raw WindNinja output under runtime/temp/
  --keep-data   Keep static_data/demo_smoke.tif after the run
USAGE
        exit 0
        ;;
      *)
        echo "Unknown demo-smoke option: $1"
        echo "Usage: ./deploy/gcp/mwn.sh demo-smoke [--keep-temp] [--keep-data]"
        exit 1
        ;;
    esac
    shift || true
  done

  pick_docker
  mkdir -p "$REPO_DIR/static_data"

  DEMO_DOMAIN_KEY="demo_smoke"
  DEMO_DEM_PATH="$REPO_DIR/static_data/${DEMO_DOMAIN_KEY}.tif"
  DEMO_KEEP_DATA="$keep_data"
  DEMO_BACKUP_DOMAINS="$(mktemp "${TMPDIR:-/tmp}/mwn-domains.XXXXXX")"
  cp "$REPO_DIR/config/domains.json" "$DEMO_BACKUP_DOMAINS"
  trap demo_smoke_cleanup EXIT

  echo "Creating synthetic demo terrain: static_data/${DEMO_DOMAIN_KEY}.tif"
  rm -f "$DEMO_DEM_PATH"
  compose run --rm shell gdal_create \
    -of GTiff \
    -outsize 200 200 \
    -a_srs EPSG:32613 \
    -a_ullr 400000 4400000 410000 4390000 \
    -burn 3000 \
    "static_data/${DEMO_DOMAIN_KEY}.tif"

  host_python ./scripts/domain_registry.py register-domain \
    "$DEMO_DOMAIN_KEY" "static_data/${DEMO_DOMAIN_KEY}.tif" --label "Demo Smoke"

  echo ""
  echo "Running demo preflight..."
  if ! cmd_check --domain "$DEMO_DOMAIN_KEY"; then
    print_preflight_guidance
    return 1
  fi

  echo ""
  echo "Running demo smoke..."
  local smoke_args=(--domain "$DEMO_DOMAIN_KEY")
  if [ "$keep_temp" = "1" ]; then
    smoke_args+=(--keep-temp)
  fi
  if ! cmd_smoke "${smoke_args[@]}"; then
    return 1
  fi

  trap - EXIT
  demo_smoke_cleanup
  echo ""
  echo "Demo smoke passed. Config restored; generated mesh cache removed."
  if [ "$keep_data" = "1" ]; then
    echo "Kept synthetic terrain: static_data/${DEMO_DOMAIN_KEY}.tif"
  fi
}

cmd_domain_help() {
  cat <<'USAGE'
Usage:
  ./deploy/gcp/mwn.sh domain create KEY --bbox NORTH EAST SOUTH WEST --terrain-source us|srtm|gmted|lcp [options]

Options:
  --label TEXT        Human-readable label for the domain
  --resolution N      DEM resolution in meters (DEM sources only)
  --output PATH       Terrain output path (default: static_data/KEY.tif or .lcp)
  --no-set-default    Register the domain without setting it as default
  --no-check          Skip preflight check after registration

Examples:
  ./deploy/gcp/mwn.sh domain create my_area --bbox 39.65 -106.0 39.55 -106.15 --terrain-source us --label "My Area"
  ./deploy/gcp/mwn.sh domain create my_area --bbox 39.65 -106.0 39.55 -106.15 --terrain-source srtm --resolution 30
  ./deploy/gcp/mwn.sh domain create my_area --bbox 39.65 -106.0 39.55 -106.15 --terrain-source lcp
USAGE
}

cmd_domain_create() {
  local domain_key="${1:-}"
  if [ "$domain_key" = "-h" ] || [ "$domain_key" = "--help" ]; then
    cmd_domain_help
    exit 0
  fi
  if [ -z "$domain_key" ]; then
    cmd_domain_help
    exit 1
  fi
  shift || true

  local north=""
  local east=""
  local south=""
  local west=""
  local terrain_source=""
  local label=""
  local resolution=""
  local output=""
  local set_default="1"
  local run_check="1"

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --bbox)
        if [ "$#" -lt 5 ]; then
          echo "--bbox requires four values: NORTH EAST SOUTH WEST"
          exit 1
        fi
        north="$2"
        east="$3"
        south="$4"
        west="$5"
        shift 4
        ;;
      --terrain-source)
        shift
        terrain_source="${1:-}"
        ;;
      --terrain-source=*)
        terrain_source="${1#--terrain-source=}"
        ;;
      --label)
        shift
        label="${1:-}"
        ;;
      --label=*)
        label="${1#--label=}"
        ;;
      --resolution)
        shift
        resolution="${1:-}"
        ;;
      --resolution=*)
        resolution="${1#--resolution=}"
        ;;
      --output)
        shift
        output="${1:-}"
        ;;
      --output=*)
        output="${1#--output=}"
        ;;
      --no-set-default)
        set_default="0"
        ;;
      --no-check)
        run_check="0"
        ;;
      -h|--help)
        cmd_domain_help
        exit 0
        ;;
      *)
        echo "Unknown domain create option: $1"
        cmd_domain_help
        exit 1
        ;;
    esac
    shift || true
  done

  if [ -z "$north" ] || [ -z "$east" ] || [ -z "$south" ] || [ -z "$west" ]; then
    echo "domain create requires --bbox NORTH EAST SOUTH WEST"
    exit 1
  fi
  if [ -z "$terrain_source" ]; then
    echo "domain create requires --terrain-source us|srtm|gmted|lcp"
    exit 1
  fi
  case "$terrain_source" in
    us|srtm|gmted|lcp) ;;
    *)
      echo "Unknown terrain source: $terrain_source (use: us, srtm, gmted, or lcp)"
      exit 1
      ;;
  esac
  if [ "$terrain_source" = "lcp" ] && [ -n "$resolution" ]; then
    echo "--resolution only applies to DEM terrain sources (us, srtm, gmted)."
    exit 1
  fi

  host_python ./scripts/domain_registry.py validate-key "$domain_key"
  validate_bbox "$north" "$east" "$south" "$west"

  if [ -z "$output" ]; then
    output="$(host_python ./scripts/domain_registry.py default-output "$domain_key" "$terrain_source")"
  fi

  local register_args=(--register-domain "$domain_key")
  if [ -n "$label" ]; then
    register_args+=(--label "$label")
  fi
  if [ "$set_default" = "1" ]; then
    register_args+=(--set-default)
  fi

  if [ "$terrain_source" = "lcp" ]; then
    cmd_fetch_lcp "$north" "$east" "$south" "$west" "$output" "${register_args[@]}"
  else
    local fetch_args=("$north" "$east" "$south" "$west" "$output" "$terrain_source")
    if [ -n "$resolution" ]; then
      fetch_args+=("$resolution")
    fi
    cmd_fetch_dem "${fetch_args[@]}" "${register_args[@]}"
  fi

  if [ "$run_check" = "1" ]; then
    echo ""
    echo "Running preflight check for domain $domain_key..."
    cmd_check --domain "$domain_key"
  fi
}

cmd_domain() {
  local subcommand="${1:-help}"
  shift 2>/dev/null || true
  case "$subcommand" in
    create) cmd_domain_create "$@" ;;
    help|-h|--help) cmd_domain_help ;;
    *)
      echo "Unknown domain command: $subcommand"
      cmd_domain_help
      exit 1
      ;;
  esac
}

cmd_shell() {
  pick_docker
  compose run --rm shell /bin/bash
}

cmd_synoptic_points() {
  pick_docker
  compose run --rm shell python ./scripts/synoptic_validation.py prepare-points "$@"
}

cmd_validate() {
  pick_docker
  compose run --rm shell python ./scripts/synoptic_validation.py compare "$@"
}

cmd_validate_rasters() {
  pick_docker
  compose run --rm shell python ./scripts/raster_validation.py "$@"
}

cmd_validate_study() {
  pick_docker
  if [ -n "${MWN_NUM_THREADS:-}" ]; then
    compose run --rm -e "MWN_NUM_THREADS=${MWN_NUM_THREADS}" shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/validation_study.py "$@"' \
      bash "$@"
  else
    compose run --rm shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/validation_study.py "$@"' \
      bash "$@"
  fi
}

cmd_validate_k0co_height_hrrr() {
  pick_docker
  if [ -n "${MWN_NUM_THREADS:-}" ]; then
    compose run --rm -e "MWN_NUM_THREADS=${MWN_NUM_THREADS}" shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/k0co_height_hrrr_validation.py "$@"' \
      bash "$@"
  else
    compose run --rm shell bash -lc \
      'source /opt/openfoam9/etc/bashrc 2>/dev/null || true
       export FOAM_USER_LIBBIN=/usr/local/lib/
       exec /opt/venv/bin/python /opt/mountain_windninja/scripts/k0co_height_hrrr_validation.py "$@"' \
      bash "$@"
  fi
}

cmd_plot_validation() {
  host_python ./scripts/validation_plots.py "$@"
}

cmd_fetch_terrain() {
  local domain_key=""
  local label=""
  local set_default="1"
  local show_help="0"
  local center_lat=""
  local center_lon=""
  local size_km=""
  local size_mi=""
  local radius_km=""
  local radius_mi=""
  local area_file=""
  local padding_km="0"
  local dem_source="us"
  local dem_resolution=""
  local dem_output=""
  local lcp_output=""
  local positional=()

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --center)
        if [ "$#" -lt 3 ]; then
          echo "--center requires two values: LAT LON"
          exit 1
        fi
        center_lat="$2"
        center_lon="$3"
        shift 2
        ;;
      --center=*)
        center_value="${1#--center=}"
        if [[ "$center_value" != *,* ]]; then
          echo "--center= requires LAT,LON"
          exit 1
        fi
        center_lat="${center_value%%,*}"
        center_lon="${center_value#*,}"
        ;;
      --size-km)
        shift
        size_km="${1:-}"
        ;;
      --size-km=*)
        size_km="${1#--size-km=}"
        ;;
      --size-mi)
        shift
        size_mi="${1:-}"
        ;;
      --size-mi=*)
        size_mi="${1#--size-mi=}"
        ;;
      --radius-km)
        shift
        radius_km="${1:-}"
        ;;
      --radius-km=*)
        radius_km="${1#--radius-km=}"
        ;;
      --radius-mi)
        shift
        radius_mi="${1:-}"
        ;;
      --radius-mi=*)
        radius_mi="${1#--radius-mi=}"
        ;;
      --area-file)
        shift
        area_file="${1:-}"
        ;;
      --area-file=*)
        area_file="${1#--area-file=}"
        ;;
      --padding-km)
        shift
        padding_km="${1:-}"
        ;;
      --padding-km=*)
        padding_km="${1#--padding-km=}"
        ;;
      --domain)
        shift
        domain_key="${1:-}"
        ;;
      --domain=*)
        domain_key="${1#--domain=}"
        ;;
      --label)
        shift
        label="${1:-}"
        ;;
      --label=*)
        label="${1#--label=}"
        ;;
      --no-set-default)
        set_default="0"
        ;;
      --dem-source)
        shift
        dem_source="${1:-}"
        ;;
      --dem-source=*)
        dem_source="${1#--dem-source=}"
        ;;
      --dem-resolution)
        shift
        dem_resolution="${1:-}"
        ;;
      --dem-resolution=*)
        dem_resolution="${1#--dem-resolution=}"
        ;;
      --dem-output)
        shift
        dem_output="${1:-}"
        ;;
      --dem-output=*)
        dem_output="${1#--dem-output=}"
        ;;
      --lcp-output)
        shift
        lcp_output="${1:-}"
        ;;
      --lcp-output=*)
        lcp_output="${1#--lcp-output=}"
        ;;
      -h|--help)
        show_help="1"
        ;;
      --*)
        echo "Unknown fetch-terrain option: $1"
        exit 1
        ;;
      *)
        positional+=("$1")
        ;;
    esac
    shift || true
  done

  if [ "$show_help" = "1" ]; then
    cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh fetch-terrain --center LAT LON --size-km N --domain KEY [options]
       ./deploy/gcp/mwn.sh fetch-terrain --area-file area.kml --domain KEY [options]
       ./deploy/gcp/mwn.sh fetch-terrain NORTH EAST SOUTH WEST --domain KEY [options]

Downloads both a DEM and an LCP for the same area. The DEM is registered first
as a fallback. The LCP is registered last, so the active domain uses the LCP
when both downloads succeed.

Area options:
  --center LAT LON       Build a square bbox centered at one point
  --size-km N            Square size in kilometers (recommended: 10)
  --size-mi N            Square size in miles
  --radius-km N          Radius from center; equivalent to --size-km 2N
  --radius-mi N          Radius from center; equivalent to --size-mi 2N
  --area-file PATH       KML/KMZ file; bbox is computed from all coordinates
  --padding-km N         Expand --area-file bbox by N kilometers

Registration:
  --domain KEY           Required. Add/update KEY in config/domains.json
  --label TEXT           Human-readable domain label
  --no-set-default       Register without changing the default domain

DEM options:
  --dem-source us|srtm|gmted  DEM source (default: us)
  --dem-resolution N          DEM output resolution in meters
  --dem-output PATH           DEM output path (default: static_data/KEY.tif)
  --lcp-output PATH           LCP output path (default: static_data/KEY.lcp)

Examples:
  ./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 --domain keystone --label "Keystone"
  ./deploy/gcp/mwn.sh fetch-terrain --area-file keystone.kml --padding-km 1 --domain keystone
  ./deploy/gcp/mwn.sh fetch-terrain 39.65 -106.0 39.55 -106.15 --domain keystone
USAGE
    exit 0
  fi

  if [ -z "$domain_key" ]; then
    echo "fetch-terrain requires --domain KEY"
    exit 1
  fi
  host_python ./scripts/domain_registry.py validate-key "$domain_key"

  local area_mode_count=0
  if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ -n "$area_file" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ "$area_mode_count" -gt 1 ]; then
    echo "Use only one area input: bbox, --center, or --area-file."
    exit 1
  fi
  if [ "$area_mode_count" -eq 0 ] && [ "${#positional[@]}" -ne 4 ]; then
    echo "fetch-terrain requires bbox N E S W, or --center LAT LON --size-km N, or --area-file PATH."
    exit 1
  fi
  if [ "$area_mode_count" -eq 1 ] && [ "${#positional[@]}" -ne 0 ]; then
    echo "fetch-terrain area modes do not accept positional arguments."
    exit 1
  fi

  local area_args=()
  if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
    area_args+=(--center "$center_lat" "$center_lon")
    if [ -n "$size_km" ]; then
      area_args+=(--size-km "$size_km")
    fi
    if [ -n "$size_mi" ]; then
      area_args+=(--size-mi "$size_mi")
    fi
    if [ -n "$radius_km" ]; then
      area_args+=(--radius-km "$radius_km")
    fi
    if [ -n "$radius_mi" ]; then
      area_args+=(--radius-mi "$radius_mi")
    fi
  elif [ -n "$area_file" ]; then
    area_args+=(--area-file "$area_file")
    if [ -n "$padding_km" ]; then
      area_args+=(--padding-km "$padding_km")
    fi
  else
    area_args+=("${positional[@]}")
  fi

  local registration_args=(--domain "$domain_key")
  if [ -n "$label" ]; then
    registration_args+=(--label "$label")
  fi
  if [ "$set_default" = "0" ]; then
    registration_args+=(--no-set-default)
  fi

  local dem_args=("${area_args[@]}")
  if [ -z "$dem_output" ] && { [ "$dem_source" != "us" ] || [ -n "$dem_resolution" ]; }; then
    dem_output="$(host_python ./scripts/domain_registry.py default-output "$domain_key" "$dem_source")"
  fi
  if [ -n "$dem_output" ]; then
    dem_args+=("$dem_output")
  fi
  if [ "$dem_source" != "us" ] || [ -n "$dem_resolution" ]; then
    dem_args+=("$dem_source")
  fi
  if [ -n "$dem_resolution" ]; then
    dem_args+=("$dem_resolution")
  fi
  dem_args+=("${registration_args[@]}")

  local lcp_args=("${area_args[@]}")
  if [ -n "$lcp_output" ]; then
    lcp_args+=("$lcp_output")
  fi
  lcp_args+=("${registration_args[@]}")

  echo "Step 1/2: downloading DEM fallback..."
  cmd_fetch_dem "${dem_args[@]}"
  echo ""
  echo "Step 2/2: downloading LCP active terrain..."
  cmd_fetch_lcp "${lcp_args[@]}"
  echo ""
  if [ "$set_default" = "1" ]; then
    echo "Terrain ready. Active domain '$domain_key' uses LCP; DEM fallback is also saved."
    echo "Next: run ./deploy/gcp/mwn.sh check && ./deploy/gcp/mwn.sh smoke"
  else
    echo "Terrain ready for domain '$domain_key'. DEM fallback and LCP are saved."
    echo "Next: run ./deploy/gcp/mwn.sh check --domain $domain_key && ./deploy/gcp/mwn.sh smoke --domain $domain_key"
  fi
}

cmd_fetch_dem() {
  local register_domain=""
  local registration_mode=""
  local label=""
  local set_default=""
  local show_help="0"
  local center_lat=""
  local center_lon=""
  local size_km=""
  local size_mi=""
  local radius_km=""
  local radius_mi=""
  local area_file=""
  local padding_km="0"
  local positional=()

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --center)
        if [ "$#" -lt 3 ]; then
          echo "--center requires two values: LAT LON"
          exit 1
        fi
        center_lat="$2"
        center_lon="$3"
        shift 2
        ;;
      --center=*)
        center_value="${1#--center=}"
        if [[ "$center_value" != *,* ]]; then
          echo "--center= requires LAT,LON"
          exit 1
        fi
        center_lat="${center_value%%,*}"
        center_lon="${center_value#*,}"
        ;;
      --size-km)
        shift
        size_km="${1:-}"
        ;;
      --size-km=*)
        size_km="${1#--size-km=}"
        ;;
      --size-mi)
        shift
        size_mi="${1:-}"
        ;;
      --size-mi=*)
        size_mi="${1#--size-mi=}"
        ;;
      --radius-km)
        shift
        radius_km="${1:-}"
        ;;
      --radius-km=*)
        radius_km="${1#--radius-km=}"
        ;;
      --radius-mi)
        shift
        radius_mi="${1:-}"
        ;;
      --radius-mi=*)
        radius_mi="${1#--radius-mi=}"
        ;;
      --area-file)
        shift
        area_file="${1:-}"
        ;;
      --area-file=*)
        area_file="${1#--area-file=}"
        ;;
      --padding-km)
        shift
        padding_km="${1:-}"
        ;;
      --padding-km=*)
        padding_km="${1#--padding-km=}"
        ;;
      --domain)
        shift
        register_domain="${1:-}"
        registration_mode="domain"
        ;;
      --domain=*)
        register_domain="${1#--domain=}"
        registration_mode="domain"
        ;;
      --register-domain)
        shift
        register_domain="${1:-}"
        registration_mode="register"
        ;;
      --register-domain=*)
        register_domain="${1#--register-domain=}"
        registration_mode="register"
        ;;
      --label)
        shift
        label="${1:-}"
        ;;
      --label=*)
        label="${1#--label=}"
        ;;
      --set-default)
        set_default="1"
        ;;
      --no-set-default)
        set_default="0"
        ;;
      -h|--help)
        show_help="1"
        positional=()
        break
        ;;
      --*)
        echo "Unknown fetch-dem option: $1"
        exit 1
        ;;
      *)
        positional+=("$1")
        ;;
    esac
    shift || true
  done

  local north=""
  local east=""
  local south=""
  local west=""
  local output=""
  local src="us"
  local resolution=""
  local area_description=""

  if [ "$show_help" = "1" ]; then
    cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh fetch-dem <north> <east> <south> <west> [output] [source] [resolution] [options]
       ./deploy/gcp/mwn.sh fetch-dem --center LAT LON --size-km N [output] [source] [resolution] [options]
       ./deploy/gcp/mwn.sh fetch-dem --area-file area.kml [output] [source] [resolution] [options]

Downloads elevation data (DEM). Source "us" uses USGS 3DEP through GDAL;
"srtm" and "gmted" use WindNinja's fetch_dem tool.

Arguments:
  north/east/south/west  Bounding box in decimal degrees (latitude/longitude)
  output                 Output file path (default: static_data/KEY.tif with
                         --domain, otherwise static_data/dem_download.tif)
  source                 Data source (default: us). Options:
                           us    - USGS 3DEP 10m (US only, no key needed, true 10m)
                           srtm  - SRTM 30m via OpenTopography (global, needs API key)
                           gmted - GMTED2010 (~250m global, no key needed)
  resolution             Output resolution in meters (default: 10 for us, 30 for srtm)

Options:
  --center LAT LON       Build a square bbox centered at one point
  --size-km N            Square size in kilometers (recommended: 10)
  --size-mi N            Square size in miles
  --radius-km N          Radius from center; equivalent to --size-km 2N
  --radius-mi N          Radius from center; equivalent to --size-mi 2N
  --area-file PATH       KML/KMZ file; bbox is computed from all coordinates
  --padding-km N         Expand --area-file bbox by N kilometers
  --domain KEY          Add/update KEY in config/domains.json and make it default
  --register-domain KEY  Add/update KEY in config/domains.json for this terrain
  --label TEXT           Human-readable domain label
  --set-default          Set KEY as default_domain and MWN_DOMAIN_ID
  --no-set-default       Register without changing the default domain

Note: srtm source requires an OpenTopography API key. Set CUSTOM_SRTM_API_KEY
in config/runtime.env. Get a free key at https://opentopography.org/

Examples:
  ./deploy/gcp/mwn.sh fetch-dem --center 39.60 -106.08 --size-km 12 --domain my_area --label "My Area"
  ./deploy/gcp/mwn.sh fetch-dem --area-file area.kml --padding-km 1 --domain my_area
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 --domain my_area --label "My Area"
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10
  ./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif srtm 30
  ./deploy/gcp/mwn.sh fetch-dem 45.5 7.0 45.0 6.5 static_data/alps.tif srtm 30
USAGE
    exit 0
  fi

  local area_mode_count=0
  if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ -n "$area_file" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ "$area_mode_count" -gt 1 ]; then
    echo "Use only one area input: bbox, --center, or --area-file."
    exit 1
  fi

  if [ "$area_mode_count" -eq 1 ]; then
    if [ "${#positional[@]}" -gt 3 ]; then
      echo "Too many positional arguments for area-based fetch-dem. Use: [output] [source] [resolution]."
      exit 1
    fi
    output="${positional[0]:-}"
    src="${positional[1]:-us}"
    resolution="${positional[2]:-}"

    local bounds
    if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
      bounds="$(resolve_center_bbox "$center_lat" "$center_lon" "$size_km" "$size_mi" "$radius_km" "$radius_mi")" || exit 1
      area_description="Area: center ${center_lat},${center_lon}"
      if [ -n "$size_km" ]; then
        area_description="${area_description} | ${size_km} km square"
      elif [ -n "$size_mi" ]; then
        area_description="${area_description} | ${size_mi} mi square"
      elif [ -n "$radius_km" ]; then
        area_description="${area_description} | ${radius_km} km radius"
      elif [ -n "$radius_mi" ]; then
        area_description="${area_description} | ${radius_mi} mi radius"
      fi
    else
      bounds="$(resolve_area_file_bbox "$area_file" "$padding_km")" || exit 1
      area_description="Area: file $area_file"
      if [ "$padding_km" != "0" ]; then
        area_description="${area_description} | ${padding_km} km padding"
      fi
    fi
    read -r north east south west <<< "$bounds"
  else
    north="${positional[0]:-}"
    east="${positional[1]:-}"
    south="${positional[2]:-}"
    west="${positional[3]:-}"
    output="${positional[4]:-}"
    src="${positional[5]:-us}"
    resolution="${positional[6]:-}"

    if [ "${#positional[@]}" -gt 7 ]; then
      echo "Too many positional arguments for fetch-dem."
      exit 1
    fi
    if [ -z "$north" ] || [ -z "$east" ] || [ -z "$south" ] || [ -z "$west" ]; then
      echo "fetch-dem requires bbox N E S W, or --center LAT LON --size-km N, or --area-file PATH."
      echo "Run ./deploy/gcp/mwn.sh fetch-dem --help for usage."
      exit 1
    fi
  fi

  if [ -n "$register_domain" ] && [ -z "$set_default" ]; then
    if [ "$registration_mode" = "domain" ]; then
      set_default="1"
    else
      set_default="0"
    fi
  elif [ -z "$set_default" ]; then
    set_default="0"
  fi

  if [ -n "$register_domain" ]; then
    host_python ./scripts/domain_registry.py validate-key "$register_domain"
  fi

  if [ -z "$output" ]; then
    if [ -n "$register_domain" ]; then
      output="$(host_python ./scripts/domain_registry.py default-output "$register_domain" "$src")"
    else
      output="static_data/dem_download.tif"
    fi
  fi

  validate_bbox "$north" "$east" "$south" "$west"
  if [ -n "$area_description" ]; then
    echo "$area_description"
    echo "BBox: north=$north east=$east south=$south west=$west"
  fi
  pick_docker

  if [ "$src" = "us" ]; then
    local res="${resolution:-10}"
    local vrt="/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"

    # Auto-detect UTM zone from center of bounding box
    local epsg
    epsg=$(awk -v n="$north" -v s="$south" -v e="$east" -v w="$west" \
      'BEGIN {
        lat = (n + s) / 2
        lon = (e + w) / 2
        zone = int((lon + 180) / 6) + 1
        if (lat >= 0) print 32600 + zone
        else print 32700 + zone
      }')

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
  register_domain_if_requested "$register_domain" "$label" "$set_default" "$output"
  if [ -n "$register_domain" ]; then
    if [ "$set_default" = "1" ]; then
      echo "Next: run ./deploy/gcp/mwn.sh check && ./deploy/gcp/mwn.sh smoke"
    else
      echo "Next: run ./deploy/gcp/mwn.sh smoke --domain $register_domain"
    fi
  else
    echo "Next: add it to config/domains.json or rerun with --domain, then run ./deploy/gcp/mwn.sh check"
  fi
}

cmd_fetch_lcp() {
  local register_domain=""
  local registration_mode=""
  local label=""
  local set_default=""
  local show_help="0"
  local center_lat=""
  local center_lon=""
  local size_km=""
  local size_mi=""
  local radius_km=""
  local radius_mi=""
  local area_file=""
  local padding_km="0"
  local positional=()

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --center)
        if [ "$#" -lt 3 ]; then
          echo "--center requires two values: LAT LON"
          exit 1
        fi
        center_lat="$2"
        center_lon="$3"
        shift 2
        ;;
      --center=*)
        center_value="${1#--center=}"
        if [[ "$center_value" != *,* ]]; then
          echo "--center= requires LAT,LON"
          exit 1
        fi
        center_lat="${center_value%%,*}"
        center_lon="${center_value#*,}"
        ;;
      --size-km)
        shift
        size_km="${1:-}"
        ;;
      --size-km=*)
        size_km="${1#--size-km=}"
        ;;
      --size-mi)
        shift
        size_mi="${1:-}"
        ;;
      --size-mi=*)
        size_mi="${1#--size-mi=}"
        ;;
      --radius-km)
        shift
        radius_km="${1:-}"
        ;;
      --radius-km=*)
        radius_km="${1#--radius-km=}"
        ;;
      --radius-mi)
        shift
        radius_mi="${1:-}"
        ;;
      --radius-mi=*)
        radius_mi="${1#--radius-mi=}"
        ;;
      --area-file)
        shift
        area_file="${1:-}"
        ;;
      --area-file=*)
        area_file="${1#--area-file=}"
        ;;
      --padding-km)
        shift
        padding_km="${1:-}"
        ;;
      --padding-km=*)
        padding_km="${1#--padding-km=}"
        ;;
      --domain)
        shift
        register_domain="${1:-}"
        registration_mode="domain"
        ;;
      --domain=*)
        register_domain="${1#--domain=}"
        registration_mode="domain"
        ;;
      --register-domain)
        shift
        register_domain="${1:-}"
        registration_mode="register"
        ;;
      --register-domain=*)
        register_domain="${1#--register-domain=}"
        registration_mode="register"
        ;;
      --label)
        shift
        label="${1:-}"
        ;;
      --label=*)
        label="${1#--label=}"
        ;;
      --set-default)
        set_default="1"
        ;;
      --no-set-default)
        set_default="0"
        ;;
      -h|--help)
        show_help="1"
        positional=()
        break
        ;;
      --*)
        echo "Unknown fetch-lcp option: $1"
        exit 1
        ;;
      *)
        positional+=("$1")
        ;;
    esac
    shift || true
  done

  local north=""
  local east=""
  local south=""
  local west=""
  local output=""
  local area_description=""

  if [ "$show_help" = "1" ]; then
    cat <<'USAGE'
Usage: ./deploy/gcp/mwn.sh fetch-lcp <north> <east> <south> <west> [output] [options]
       ./deploy/gcp/mwn.sh fetch-lcp --center LAT LON --size-km N [output] [options]
       ./deploy/gcp/mwn.sh fetch-lcp --area-file area.kml [output] [options]

Downloads an LCP (Landscape) file from LANDFIRE. LCP files include elevation
plus vegetation/fuel data (8 bands), giving WindNinja real vegetation info
instead of a uniform assumption.

Arguments:
  north/east/south/west  Bounding box in decimal degrees (latitude/longitude)
  output                 Output file path (default: static_data/KEY.lcp with
                         --domain, otherwise static_data/lcp_download.lcp)

Options:
  --center LAT LON       Build a square bbox centered at one point
  --size-km N            Square size in kilometers (recommended: 10)
  --size-mi N            Square size in miles
  --radius-km N          Radius from center; equivalent to --size-km 2N
  --radius-mi N          Radius from center; equivalent to --size-mi 2N
  --area-file PATH       KML/KMZ file; bbox is computed from all coordinates
  --padding-km N         Expand --area-file bbox by N kilometers
  --domain KEY          Add/update KEY in config/domains.json and make it default
  --register-domain KEY  Add/update KEY in config/domains.json for this terrain
  --label TEXT           Human-readable domain label
  --set-default          Set KEY as default_domain and MWN_DOMAIN_ID
  --no-set-default       Register without changing the default domain

Note: LANDFIRE downloads can take several minutes due to server processing.
Only available for the United States.

Examples:
  ./deploy/gcp/mwn.sh fetch-lcp --center 39.60 -106.08 --size-km 12 --domain my_area --label "My Area"
  ./deploy/gcp/mwn.sh fetch-lcp --area-file area.kml --padding-km 1 --domain my_area
  ./deploy/gcp/mwn.sh fetch-lcp 40.0 -105.0 39.5 -105.5
  ./deploy/gcp/mwn.sh fetch-lcp 40.0 -105.0 39.5 -105.5 static_data/my_area.lcp
  ./deploy/gcp/mwn.sh fetch-lcp 40.0 -105.0 39.5 -105.5 --domain my_area --label "My Area"
USAGE
    exit 0
  fi

  local area_mode_count=0
  if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ -n "$area_file" ]; then
    area_mode_count=$((area_mode_count + 1))
  fi
  if [ "$area_mode_count" -gt 1 ]; then
    echo "Use only one area input: bbox, --center, or --area-file."
    exit 1
  fi

  if [ "$area_mode_count" -eq 1 ]; then
    if [ "${#positional[@]}" -gt 1 ]; then
      echo "Too many positional arguments for area-based fetch-lcp. Use: [output]."
      exit 1
    fi
    output="${positional[0]:-}"

    local bounds
    if [ -n "$center_lat" ] || [ -n "$center_lon" ]; then
      bounds="$(resolve_center_bbox "$center_lat" "$center_lon" "$size_km" "$size_mi" "$radius_km" "$radius_mi")" || exit 1
      area_description="Area: center ${center_lat},${center_lon}"
      if [ -n "$size_km" ]; then
        area_description="${area_description} | ${size_km} km square"
      elif [ -n "$size_mi" ]; then
        area_description="${area_description} | ${size_mi} mi square"
      elif [ -n "$radius_km" ]; then
        area_description="${area_description} | ${radius_km} km radius"
      elif [ -n "$radius_mi" ]; then
        area_description="${area_description} | ${radius_mi} mi radius"
      fi
    else
      bounds="$(resolve_area_file_bbox "$area_file" "$padding_km")" || exit 1
      area_description="Area: file $area_file"
      if [ "$padding_km" != "0" ]; then
        area_description="${area_description} | ${padding_km} km padding"
      fi
    fi
    read -r north east south west <<< "$bounds"
  else
    north="${positional[0]:-}"
    east="${positional[1]:-}"
    south="${positional[2]:-}"
    west="${positional[3]:-}"
    output="${positional[4]:-}"

    if [ "${#positional[@]}" -gt 5 ]; then
      echo "Too many positional arguments for fetch-lcp."
      exit 1
    fi
    if [ -z "$north" ] || [ -z "$east" ] || [ -z "$south" ] || [ -z "$west" ]; then
      echo "fetch-lcp requires bbox N E S W, or --center LAT LON --size-km N, or --area-file PATH."
      echo "Run ./deploy/gcp/mwn.sh fetch-lcp --help for usage."
      exit 1
    fi
  fi

  if [ -n "$register_domain" ] && [ -z "$set_default" ]; then
    if [ "$registration_mode" = "domain" ]; then
      set_default="1"
    else
      set_default="0"
    fi
  elif [ -z "$set_default" ]; then
    set_default="0"
  fi

  if [ -n "$register_domain" ]; then
    host_python ./scripts/domain_registry.py validate-key "$register_domain"
  fi

  if [ -z "$output" ]; then
    if [ -n "$register_domain" ]; then
      output="$(host_python ./scripts/domain_registry.py default-output "$register_domain" lcp)"
    else
      output="static_data/lcp_download.lcp"
    fi
  fi

  validate_bbox "$north" "$east" "$south" "$west"
  if [ -n "$area_description" ]; then
    echo "$area_description"
    echo "BBox: north=$north east=$east south=$south west=$west"
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
  compose run --rm shell bash -lc 'gdalsrsinfo -o wkt "$1" > "$2"' bash "$output" "$prj_path"

  echo ""
  echo "LCP saved to $output"
  register_domain_if_requested "$register_domain" "$label" "$set_default" "$output"
  if [ -n "$register_domain" ]; then
    if [ "$set_default" = "1" ]; then
      echo "Next: run ./deploy/gcp/mwn.sh check && ./deploy/gcp/mwn.sh smoke"
    else
      echo "Next: run ./deploy/gcp/mwn.sh smoke --domain $register_domain"
    fi
  else
    echo "Next: add it to config/domains.json or rerun with --domain, then run ./deploy/gcp/mwn.sh check"
  fi
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
  echo "Scheduler started. Runs use MWN_SCHEDULE_* settings from config/runtime.env."
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
  MWN_DOCKER_IMAGE=mountain-windninja:local compose build
  set_runtime_env MWN_DOCKER_IMAGE "mountain-windninja:local"
  MWN_DOCKER_IMAGE=mountain-windninja:local compose up -d scheduler 2>/dev/null || true
  echo "Updated and rebuilt."
}

# ── Dispatch ───────────────────────────────────────────────────────────────

COMMAND="${1:-help}"
shift 2>/dev/null || true

case "$COMMAND" in
  help|-h|--help)  cmd_help "$@" ;;
  init)            cmd_init "$@" ;;
  build)           cmd_build ;;
  build-local)     cmd_build_local ;;
  pull)            cmd_pull "$@" ;;
  check)           cmd_check "$@" ;;
  run)             cmd_run "$@" ;;
  run-grid)        cmd_run_grid "$@" ;;
  smoke)           cmd_smoke "$@" ;;
  demo-smoke)      cmd_demo_smoke "$@" ;;
  domain)          cmd_domain "$@" ;;
  shell)           cmd_shell ;;
  synoptic-points) cmd_synoptic_points "$@" ;;
  validate)        cmd_validate "$@" ;;
  validate-rasters) cmd_validate_rasters "$@" ;;
  validate-study)  cmd_validate_study "$@" ;;
  validate-k0co-height-hrrr) cmd_validate_k0co_height_hrrr "$@" ;;
  plot-validation) cmd_plot_validation "$@" ;;
  forcing-from-grib) cmd_forcing_from_grib "$@" ;;
  clean)           cmd_clean ;;
  fetch-terrain)   cmd_fetch_terrain "$@" ;;
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

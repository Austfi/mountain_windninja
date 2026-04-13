# Mountain WindNinja

Run [WindNinja](https://research.fs.usda.gov/firelab/products/dataandtools/windninja) on a cloud server with simple terminal commands. WindNinja simulates how wind flows over complex terrain. This project packages it inside Docker so you can run it on a Google Cloud VM (or any Linux server) without installing anything manually.

**Output:** KMZ files for Google Earth (animated wind vectors over terrain) + ASCII grids (raw speed/direction data).

**New to Google Cloud?** New accounts get **$300 free credits** (90 days). An `e2-standard-4` VM costs ~$0.13/hour. See the [GCP setup guide](docs/gcp_setup.md) for VM sizing, cost tips, and step-by-step instructions.

## Quick Start

**You need:** The lat/lon bounding box for your area of interest.

```bash
# 1. Clone and set up
git clone https://github.com/Austfi/mountain_windninja.git /opt/mountain_windninja
cd /opt/mountain_windninja
./deploy/gcp/bootstrap_repo.sh

# 2. Build Docker image (~30 min first time, needs 50 GB disk)
./deploy/gcp/mwn.sh build

# 3. Download terrain data for your area
#    Option A: USGS 3DEP 10m (US only, no API key needed)
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10

#    Option B: SRTM 30m via OpenTopography (global, needs free API key)
#    Set CUSTOM_SRTM_API_KEY in config/runtime.env first
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif srtm 30

#    Option C: LANDFIRE LCP (US only, includes vegetation/fuel data)
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/my_area.lcp

# 4. Edit config/domains.json -- set elevation_file to your filename
nano config/domains.json

# 5. Verify setup and run
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh run --hours 6
```

Output goes to `runtime/archives/` (zipped KMZ files, ASCII grids, and run config). Full walkthrough: [docs/gcp_setup.md](docs/gcp_setup.md).

## How It Works

1. You provide a terrain file (DEM `.tif` or LCP `.lcp`) in `static_data/`
2. You define a domain in `config/domains.json` pointing to your terrain + the config template
3. `mwn.sh run` starts a Docker container with WindNinja + OpenFOAM, generates config, runs simulation
4. WindNinja downloads forecast data from NOAA automatically, and reanalysis data from the public HRRR archive
5. Output files (KMZ, ASCII grids) go to `runtime/`

## Three Run Modes

```bash
# Forecast: live weather data from NOAA (default)
./deploy/gcp/mwn.sh run --hours 18 --model HRRR

# Reanalysis: archived past weather (HRRR only, back to 2014)
./deploy/gcp/mwn.sh run --mode reanalysis --hours 12
./deploy/gcp/mwn.sh run --mode reanalysis --start 202601010000 --end 202601080000

# Domain-average: manual wind input, no internet needed
./deploy/gcp/mwn.sh run --mode domain-average --speed 20 --direction 270
```

## Validation Workflow

You can validate a historical WindNinja run against Synoptic stations and against the parent HRRR vectors at the exact same station points.

Notes:
- Reanalysis uses the patched Docker image in this repo and should not require manual GCS credentials for public HRRR archive access after `./deploy/gcp/mwn.sh build`.
- Validation requires a working Synoptic token with data access.

```bash
# 1. Build a WindNinja points CSV from a Synoptic station manifest
./deploy/gcp/mwn.sh synoptic-points \
  --station-file config/stations/loveland_pass_validation_manifest.csv \
  --points-output runtime/validation/loveland_points.csv \
  --metadata-output runtime/validation/loveland_metadata.json \
  --start 202601010000 --end 202601080000

# 2. Run a fixed historical HRRR archive window with point sampling enabled
./deploy/gcp/mwn.sh run --mode reanalysis \
  --start 202601010000 --end 202601080000 \
  --model HRRR \
  --points-file runtime/validation/loveland_points.csv \
  --keep-temp --no-upload

# 3. Compare Synoptic observations vs WindNinja and raw HRRR at the same points
./deploy/gcp/mwn.sh validate \
  --points-output runtime/temp/20260101_reanalysis_168h_HRRR/my_area_sample_points.csv \
  --metadata-file runtime/validation/loveland_metadata.json \
  --start 202601010000 --end 202601080000 \
  --samples-csv runtime/validation/jan2026_samples.csv \
  --station-summary-csv runtime/validation/jan2026_station_summary.csv \
  --group-summary-csv runtime/validation/jan2026_group_summary.csv \
  --summary-json runtime/validation/jan2026_summary.json
```

The validation outputs include per-sample comparisons, per-station summaries, grouped summaries, and an overall JSON summary. WindNinja point sampling carries both `u,v` and parent-weather-model `wx_u,wx_v`, so the baseline HRRR comparison is generated from the same station/time matches as the downscaled output.

## Commands


| Command                                      | What it does                                  |
| -------------------------------------------- | --------------------------------------------- |
| `mwn.sh build`                               | Build Docker image                            |
| `mwn.sh check`                               | Verify setup                                  |
| `mwn.sh run [flags]`                         | Run simulation                                |
| `mwn.sh shell`                               | Open container shell                          |
| `mwn.sh fetch-dem N E S W [out] [src] [res]` | Download DEM (source: `us`, `srtm`, `gmted`) |
| `mwn.sh clean`                               | Clear cached mesh and temp files              |
| `mwn.sh fetch-lcp N E S W [out]`             | Download LCP from LANDFIRE (US only)          |
| `mwn.sh lcp-build input.tif [out.lcp]`       | Convert LANDFIRE GeoTIFF to LCP               |
| `mwn.sh synoptic-points`                     | Build WindNinja point CSV from Synoptic metadata |
| `mwn.sh validate`                            | Compare WindNinja/HRRR point output vs Synoptic |
| `mwn.sh schedule`                            | Start hourly auto-forecasts                   |
| `mwn.sh stop`                                | Stop scheduler                                |
| `mwn.sh logs`                                | View scheduler logs                           |
| `mwn.sh update`                              | Pull code + rebuild                           |


Full flags, weather models, and examples: [docs/commands.md](docs/commands.md).

## Terrain Data

WindNinja needs elevation data for your area. Three ways to get it:

### Option 1: DEM (elevation only)

```bash
# USGS 3DEP -- true 10m resolution, US only, no API key needed
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10

# SRTM -- 30m resolution, global coverage, needs free OpenTopography API key
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif srtm 30

# GMTED -- coarser (~250m), global, good for large areas
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif gmted
```

WindNinja applies a uniform vegetation type (`grass`, `brush`, or `trees` -- set via `MWN_SURFACE_VEGETATION` in `config/runtime.env`).

### Option 2: LCP from LANDFIRE (elevation + vegetation)

LCP files include elevation plus 7 bands of vegetation/fuel data. More accurate than DEM + uniform vegetation. US only.

```bash
# Download directly from LANDFIRE (may take several minutes)
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/my_area.lcp
```

### Option 3: Convert a LANDFIRE GeoTIFF to LCP

If you downloaded a landscape GeoTIFF from [LANDFIRE](https://www.landfire.gov/viewer/) or [IFTDSS](https://iftdss.firenet.gov/), convert it to LCP format:

```bash
./deploy/gcp/mwn.sh lcp-build static_data/landscape_download.tif static_data/my_area.lcp
```

This creates both the `.lcp` file and its required `.prj` sidecar.

### After downloading terrain

Edit `config/domains.json` to point to your file:

```json
{
  "default_domain": "my_area",
  "domains": {
    "my_area": {
      "label": "My Area",
      "template": "config/template.cfg",
      "elevation_file": "my_area.tif"
    }
  }
}
```

Make sure `MWN_DOMAIN_ID` in `config/runtime.env` matches the domain key. Run `./deploy/gcp/mwn.sh check` to verify.

### DEM vs LCP


|                  | DEM (.tif)                                | LCP (.lcp)                                  |
| ---------------- | ----------------------------------------- | ------------------------------------------- |
| **Data**         | Elevation only                            | Elevation + vegetation/fuel (8 bands)       |
| **Accuracy**     | Good (uniform vegetation assumption)      | Best (real vegetation data)                 |
| **Availability** | US (3DEP 10m), global (SRTM 30m)          | US only (LANDFIRE)                          |
| **Best for**     | Quick runs, non-US areas                  | Fire modeling, forested terrain, production |


For detailed terrain instructions, see the [terrain data guide](docs/gcp_setup.md#step-6-get-your-terrain-data).

## Project Layout

```
config/
  domains.json         # Domain catalog (name → terrain file + template)
  template.cfg         # WindNinja config template (shared by all domains)
  runtime.env          # Your settings (created from .example)

static_data/           # Terrain files (DEM .tif or LCP .lcp)
runtime/               # Output (auto-created): archives/, temp/, logs/

scripts/
  daily_run.py         # Core: generates config, runs WindNinja, archives output
  config_loader.py     # Loads settings from runtime.env + domains.json
  run_windninja.sh     # Entry wrapper (sources OpenFOAM env)
  gcs_manager.py       # Optional GCS upload
  preflight_check.py   # Readiness checks
  synoptic_validation.py  # Station metadata prep + validation metrics
  build_lcp_from_geotiff.py  # GeoTIFF → LCP converter

deploy/gcp/
  mwn.sh               # Main CLI
  bootstrap_repo.sh    # One-time server setup

docker/
  patch_windninja_public_pastcast.py  # Build-time upstream WindNinja patch for public HRRR pastcast

docs/
  gcp_setup.md         # Full GCP walkthrough + cost guide + troubleshooting
  commands.md          # Detailed command reference
  agent_handoff.md     # Current repo state, known constraints, handoff checklist
  windninja_reference.md  # WindNinja internals reference
```

## Key Config Settings

Edit `config/template.cfg` to tune WindNinja behavior:


| Setting                | What it controls                                         | Default |
| ---------------------- | -------------------------------------------------------- | ------- |
| `mesh_resolution`      | Grid cell size in meters (smaller = more detail, slower) | `100.0` |
| `momentum_flag`        | OpenFOAM solver (`true` = better accuracy, slower)       | `true`  |
| `num_threads`          | CPU threads                                              | `4`     |
| `number_of_iterations` | Solver iterations                                        | `300`   |
| `diurnal_winds`        | Thermal slope winds                                      | `true`  |
| `output_wind_height`   | Height above ground for output (set via `--height` flag) | `10.0`  |


## Docs

- **[Beginner Tutorial](docs/tutorial.md)** -- start here. Step-by-step walkthrough from zero to your first forecast.
- **[GCP Setup Guide](docs/gcp_setup.md)** -- VM creation, costs, terrain data, troubleshooting
- **[Command Reference](docs/commands.md)** -- all flags, weather models, examples
- **[Agent Handoff](docs/agent_handoff.md)** -- current repo capabilities, operational caveats, and handoff checklist
- **[WindNinja Reference](docs/windninja_reference.md)** -- internals, config options, upstream docs

## License

WindNinja is developed by the USDA Forest Service, Rocky Mountain Research Station, Missoula Fire Sciences Laboratory. This project wraps WindNinja for cloud deployment and is not affiliated with or endorsed by the USDA.

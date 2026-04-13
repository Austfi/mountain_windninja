# Command Reference

All commands are run from the repo root:

```bash
cd /opt/mountain_windninja
./deploy/gcp/mwn.sh <command> [options]
```

## build

Build the Docker image. Takes ~30 minutes the first time (compiles WindNinja, OpenFOAM, GDAL from source). After that, rebuilds are fast unless the Dockerfile changes.

```bash
./deploy/gcp/mwn.sh build
```

Rebuild after any change to `Dockerfile` or `docker/`. Python/script-only changes under `scripts/`, `config/`, or `docs/` do not need a rebuild.

## check

Run preflight checks to verify everything is set up correctly. Checks for: WindNinja binary, OpenFOAM, GDAL, terrain files, writable directories, and domain configuration.

```bash
./deploy/gcp/mwn.sh check
```

## run

Run a WindNinja simulation. This is the main command you'll use. There are three modes: **forecast**, **reanalysis** (historical), and **domain-average** (manual wind input).

### Forecast Mode (default)

Downloads live weather data from NOAA and simulates future wind:

```bash
./deploy/gcp/mwn.sh run
./deploy/gcp/mwn.sh run --hours 6
./deploy/gcp/mwn.sh run --model NBM --hours 12
./deploy/gcp/mwn.sh run --model GFS --hours 48
./deploy/gcp/mwn.sh run --domain summit_county
```

### Reanalysis Mode (Historical)

Uses archived HRRR data from Google Cloud to simulate past wind events. Useful for analyzing fire incidents, avalanche conditions, or any past weather event. The HRRR archive goes back to 2014.

```bash
./deploy/gcp/mwn.sh run --mode reanalysis --hours 12
./deploy/gcp/mwn.sh run --mode reanalysis --hours 24
./deploy/gcp/mwn.sh run --mode reanalysis --start 202601010000 --end 202601080000
```

Only HRRR is available for reanalysis (other models don't have public archives accessible through WindNinja's pastcast system).

Use `--start` and `--end` to pin a specific historical UTC window. Both values must be hour-aligned and use either `YYYYMMDDHHMM` or `YYYY-MM-DDTHH:MM`.

The patched Docker image in this repo reads the public HRRR archive without requiring GCS keys. If you pull a change that updates `Dockerfile`, rebuild once with `./deploy/gcp/mwn.sh build` before retrying reanalysis.
If you still see `Missing required GCS credentials` during reanalysis, the running image is stale and needs to be rebuilt.

### Domain-Average Mode (Manual Wind)

Specify a single wind speed and direction. WindNinja applies it uniformly then adjusts for terrain effects. No internet/weather download needed. This is the same as the desktop app's "Domain Average" initialization.

```bash
./deploy/gcp/mwn.sh run --mode domain-average --speed 15 --direction 270
./deploy/gcp/mwn.sh run --mode domain-average --speed 10 --direction 180 --speed-units mps
./deploy/gcp/mwn.sh run --mode domain-average --speed 25 --direction 315 --keep-temp
```

Wind direction is in degrees: 0 = North, 90 = East, 180 = South, 270 = West.

### Run Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--mode forecast\|reanalysis\|domain-average` | Run mode | `forecast` |
| `--model HRRR\|NBM\|NAM\|RAP\|GFS` | Weather model (forecast/reanalysis only) | `HRRR` |
| `--hours N` | Number of hours to simulate | `18` |
| `--start UTC` | Fixed reanalysis start time | none |
| `--end UTC` | Fixed reanalysis end time | none |
| `--domain NAME` | Domain key from `config/domains.json` | Value of `MWN_DOMAIN_ID` |
| `--speed N` | Wind speed (domain-average only) | required |
| `--direction N` | Wind direction in degrees (domain-average only) | required |
| `--speed-units mph\|mps\|kph\|kts` | Units for `--speed` | `mph` |
| `--height N` | Output height above ground in meters | `10` |
| `--points-file PATH` | WindNinja WGS84 point sampling CSV | off |
| `--points-output PATH` | Output CSV for sampled `u,v,wx_u,wx_v` | auto in run dir |
| `--keep-temp` | Don't archive, keep raw files in `runtime/temp/` | off |
| `--no-upload` | Don't upload to GCS | off |
| `--dry-run` | Generate config only | off |

### Weather Models

| Short Name | Full WindNinja Name | Resolution | Domain | Best For |
|-----------|-------------------|-----------|--------|----------|
| `HRRR` | `NOMADS-HRRR-CONUS-3-KM` | 3 km | CONUS (US) | Best detail, hourly updates, up to 18h |
| `NBM` | `NOMADS-NBM-CONUS-2.5-KM` | 2.5 km | CONUS (US) | Calibrated blend of models, most accurate |
| `NAM` | `NOMADS-NAM-NEST-CONUS-3-KM` | 3 km | CONUS (US) | Longer range, 4x daily updates |
| `NAM-CONUS` | `NOMADS-NAM-CONUS-12-KM` | 12 km | CONUS (US) | Coarser NAM, longer forecasts |
| `NAM-ALASKA` | `NOMADS-NAM-ALASKA-11.25-KM` | 11.25 km | Alaska | Alaska coverage |
| `RAP` | `NOMADS-RAP-CONUS-13-KM` | 13 km | CONUS (US) | Rapid refresh, hourly, 21h range |
| `GFS` | `NOMADS-GFS-GLOBAL-0.25-DEG` | ~25 km | Global | Worldwide, long-range up to 16 days |

**Which model should I use?**
- For US mountain terrain with short forecasts: **HRRR** (default, best resolution)
- For the most statistically accurate forecast: **NBM** (blends multiple models)
- For longer-range planning (1-7 days): **GFS**
- For Alaska: **NAM-ALASKA**
- For historical analysis of past events: **HRRR** with `--mode reanalysis`

### What Happens During a Run

1. Reads your domain config (terrain file + template)
2. Fills in the template with start/stop times and paths
3. Runs `WindNinja_cli` which downloads weather data and runs the simulation
4. Bundles hourly KMZ files into a single playable KMZ
5. Archives KMZ files, ASCII grids, and generated config as a zip in `runtime/archives/`
6. If GCS is enabled, uploads the archive and latest KMZ

For domain-average mode, step 3 skips the weather download and uses the speed/direction you provided.

### Output Files

After a run with `--keep-temp`, you'll find in `runtime/temp/<run_dir>/`:

- `*.kmz` -- individual hourly KMZ files for Google Earth
- `*_vel.asc` -- wind speed ASCII grids (one per hour)
- `*_ang.asc` -- wind direction ASCII grids (one per hour)
- `*.cfg` -- the generated WindNinja config file
- `*_playable.kmz` -- all hours bundled into one KMZ with a time slider
- `*_sample_points.csv` -- sampled WindNinja and raw weather-model vectors at requested station points

Without `--keep-temp`, only archive zip is kept in `runtime/archives/`, but it still contains these run outputs.

## shell

Open an interactive bash shell inside the container. Useful for manual WindNinja runs, inspecting files, or debugging.

```bash
./deploy/gcp/mwn.sh shell

# Inside the container, you can run WindNinja directly:
source /opt/openfoam9/etc/bashrc
WindNinja_cli my_config.cfg
```

## fetch-dem

Download a DEM (Digital Elevation Model) from USGS SRTM directly into `static_data/`. Uses WindNinja's built-in `fetch_dem` tool, which downloads from OpenTopography.

```bash
# Download DEM for an area near Keystone, CO
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15

# Custom output path
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/keystone.tif

# Lower resolution (90m instead of 30m default)
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/keystone.tif 90
```

Arguments: `<north> <east> <south> <west> [output_path] [resolution_meters]`

The bounding box uses decimal degrees (latitude/longitude). You can find coordinates using Google Maps (right-click any point to see lat/lon).

**Note:** SRTM downloads require an OpenTopography API key. Get a free one at [opentopography.org](https://opentopography.org/) and set it in `config/runtime.env`:

```
CUSTOM_SRTM_API_KEY=your_key_here
```

## fetch-lcp

Download an LCP (Landscape) file from LANDFIRE. LCP files include elevation plus 7 additional bands of vegetation/fuel data, giving WindNinja much more accurate results than a bare DEM. Only available for the United States.

```bash
# Download LCP for an area
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15

# Custom output path
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/keystone.lcp
```

Arguments: `<north> <east> <south> <west> [output_path]`

LANDFIRE downloads can take several minutes because the server processes your request. The download includes elevation, slope, aspect, fire behavior fuel model, canopy cover, canopy height, crown base height, and crown bulk density.

## lcp-build

Convert a GeoTIFF with 8+ landscape bands into an LCP (Landscape) file that WindNinja can use for vegetation-aware simulations.

```bash
# Default output: static_data/summit_county_surface.lcp
./deploy/gcp/mwn.sh lcp-build static_data/landscape.tif

# Custom output path
./deploy/gcp/mwn.sh lcp-build static_data/landscape.tif static_data/my_area.lcp
```

The input GeoTIFF must have at least 8 bands: elevation, slope, aspect, fuel model, canopy cover, canopy height, crown base height, crown bulk density.

## upload

Manually trigger a GCS index update. Useful if you want to push the bucket index without running a full forecast.

```bash
./deploy/gcp/mwn.sh upload
```

Requires `MWN_GCS_UPLOAD_ENABLED=true` and `MWN_GCS_BUCKET` set in `config/runtime.env`.

## synoptic-points

Fetch Synoptic station metadata, infer wind sensor heights, and build a WindNinja point-sampling CSV.

```bash
./deploy/gcp/mwn.sh synoptic-points \
  --station-file config/stations/loveland_pass_validation_manifest.csv \
  --points-output runtime/validation/loveland_points.csv \
  --metadata-output runtime/validation/loveland_metadata.json \
  --start 202601010000 --end 202601080000
```

The station manifest is a CSV with `station_id,label,group,height_m_override`. If `height_m_override` is empty, the command uses the Synoptic wind sensor `position` metadata. It also prints a suggested padded bbox for the station set.

Requires `MWN_SYNOPTIC_TOKEN` (or `CUSTOM_API_KEY`) in `config/runtime.env`, unless you pass `--token`.
The token also needs account-level access to Synoptic weather data. A syntactically valid token with insufficient account permissions will still fail.

## validate

Compare a WindNinja `output_points_file` CSV against Synoptic observations and the parent HRRR vectors sampled at the same points.

```bash
./deploy/gcp/mwn.sh validate \
  --points-output runtime/temp/20260101_reanalysis_168h_HRRR/my_area_sample_points.csv \
  --metadata-file runtime/validation/loveland_metadata.json \
  --start 202601010000 --end 202601080000 \
  --samples-csv runtime/validation/jan2026_samples.csv \
  --station-summary-csv runtime/validation/jan2026_station_summary.csv \
  --group-summary-csv runtime/validation/jan2026_group_summary.csv \
  --summary-json runtime/validation/jan2026_summary.json
```

Outputs:
- `samples.csv` with matched per-time station rows
- `station_summary.csv` with per-station MAE/RMSE
- `group_summary.csv` with grouped MAE/RMSE
- `summary.json` with overall metrics and WindNinja-vs-HRRR improvement deltas

Validation is point-based, not raster-wide. WindNinja solves the full terrain domain, then compares model output only at the requested station/sample points where observations exist.

## schedule

Start the automatic scheduler. Runs an 18-hour HRRR forecast at :15 past every hour.

```bash
./deploy/gcp/mwn.sh schedule
```

The scheduler runs as a background Docker container. It persists across reboots.

## stop

Stop the scheduler.

```bash
./deploy/gcp/mwn.sh stop
```

## logs

View live scheduler logs.

```bash
./deploy/gcp/mwn.sh logs
# Press Ctrl+C to stop following
```

## update

Pull the latest code from git and rebuild the Docker image.

```bash
./deploy/gcp/mwn.sh update
```

This does `git pull --ff-only`, rebuilds the image, and restarts the scheduler if it was running.

## Configuration

### runtime.env

All settings are in `config/runtime.env`. Copy from the example:

```bash
cp config/runtime.container.env.example config/runtime.env
```

| Setting | Description | Default |
|---------|-------------|---------|
| `MWN_DOMAIN_ID` | Active domain (must match key in `domains.json`) | `small` |
| `MWN_RUNTIME_ROOT` | Where output files go | `runtime` |
| `MWN_STATIC_DATA_ROOT` | Where terrain files live | `static_data` |
| `MWN_WINDNINJA_CLI` | Path to WindNinja binary | `/usr/local/bin/WindNinja_cli` |
| `MWN_OPENFOAM_BASHRC` | OpenFOAM environment setup | `/opt/openfoam9/etc/bashrc` |
| `MWN_SURFACE_VEGETATION` | Default vegetation for DEM runs (grass/brush/trees) | `trees` |
| `MWN_GCS_BUCKET` | GCS bucket name for uploads | empty |
| `MWN_GCS_UPLOAD_ENABLED` | Enable GCS uploads | `false` |
| `CUSTOM_SRTM_API_KEY` | OpenTopography API key for DEM downloads | empty |

### domains.json

Defines your simulation domains. Each domain has:

- `label` -- human-readable name
- `template` -- path to a WindNinja config template
- `elevation_file` -- terrain filename (looked up in `static_data/`)

```json
{
  "default_domain": "small",
  "domains": {
    "small": {
      "label": "Keystone small placeholder domain",
      "template": "config/template.cfg",
      "elevation_file": "keystone_square_30m.tif"
    }
  }
}
```

### WindNinja Templates (.cfg)

The template files in `config/` control WindNinja's behavior. Key settings:

| Setting | What it controls |
|---------|-----------------|
| `initialization_method` | `wxModelInitialization` (weather models) or `domainAverageInitialization` (manual) |
| `mesh_resolution` | Grid resolution in meters (smaller = more detail, slower) |
| `momentum_flag` | `true` for OpenFOAM momentum solver (better accuracy, slower) |
| `number_of_iterations` | Solver iterations (300 is a good default) |
| `num_threads` | CPU threads for parallel processing |
| `diurnal_winds` | Include thermal/slope wind effects (time-of-day dependent) |
| `output_speed_units` | `mph` or `mps` (meters per second) |
| `output_wind_height` | Height above ground for output wind (default: 10m) |
| `write_goog_output` | Generate KMZ files for Google Earth |
| `write_ascii_output` | Generate ASCII grid files |
| `write_shapefile_output` | Generate shapefiles for GIS tools |
| `write_pdf_output` | Generate printable wind maps |
| `write_vtk_output` | Generate 3D VTK output |

### Initialization Methods Explained

WindNinja supports four initialization methods. This project uses the first two through the `--mode` flag:

| Method | `--mode` Flag | Description |
|--------|--------------|-------------|
| Weather Model | `forecast` or `reanalysis` | Downloads NWS weather data and downscales it to your terrain. Best for real predictions. |
| Domain Average | `domain-average` | You provide one wind speed/direction. WindNinja distributes it and adjusts for terrain. No internet needed. |
| Point | (use `mwn.sh shell`) | Supply weather station observations. WindNinja interpolates between stations. Advanced use. |
| Gridded | (use `mwn.sh shell`) | Supply your own wind speed/direction grids in .asc format. Advanced use. |

For Point and Gridded initialization, open a shell (`mwn.sh shell`) and write a custom `.cfg` file. See the [WindNinja wiki](https://github.com/firelab/windninja/wiki) for config file formats.

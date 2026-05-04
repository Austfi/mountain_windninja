# Command Reference

All commands are run from the repo root:

```bash
cd /opt/mountain_windninja
./deploy/gcp/mwn.sh <command> [options]
```

## init

Create local runtime directories and `config/runtime.env` when missing. Existing
local config is preserved.

```bash
./deploy/gcp/mwn.sh init
./deploy/gcp/mwn.sh init --image pull
./deploy/gcp/mwn.sh init --image local
./deploy/gcp/mwn.sh init --image skip
```

Image modes:

| Mode | Behavior |
|------|----------|
| `pull` | Default. Attempts to pull the default GHCR image. If it fails, build locally. |
| `local` | Uses `mountain-windninja:local`; build with `build-local`. |
| `skip` | Only creates dirs/config; leaves image settings unchanged. |

## build / build-local

Build the Docker image locally. Takes ~30 minutes the first time (compiles WindNinja, OpenFOAM, GDAL from source). After that, rebuilds are fast unless the Dockerfile changes.

```bash
./deploy/gcp/mwn.sh build
./deploy/gcp/mwn.sh build-local
```

Rebuild after any change to `Dockerfile` or `docker/`. Python/script-only changes under `scripts/`, `config/`, or `docs/` do not need a rebuild.

## demo-smoke

Prove Docker, GDAL, WindNinja, OpenFOAM, and the Python wrapper before
downloading real terrain.

```bash
./deploy/gcp/mwn.sh demo-smoke
./deploy/gcp/mwn.sh demo-smoke --keep-temp
./deploy/gcp/mwn.sh demo-smoke --keep-temp --keep-data
```

The command:

1. Creates a synthetic 10 km DEM at `static_data/demo_smoke.tif`
2. Temporarily registers a `demo_smoke` domain
3. Runs `check --domain demo_smoke`
4. Runs the deterministic `smoke` path
5. Restores `config/domains.json`
6. Removes generated OpenFOAM mesh cache

Options:

| Flag | Description |
|------|-------------|
| `--keep-temp` | Keep raw run output under `runtime/temp/` |
| `--keep-data` | Keep the generated synthetic DEM |

## pull

Pull a published image and record it in `config/runtime.env` as `MWN_DOCKER_IMAGE`. The default is `ghcr.io/austfi/mountain-windninja:3.12.2`.

```bash
./deploy/gcp/mwn.sh pull
./deploy/gcp/mwn.sh pull ghcr.io/austfi/mountain-windninja:3.12.2
```

## check

Run preflight checks to verify everything is set up correctly. Checks for: WindNinja binary, OpenFOAM, GDAL, terrain files, writable directories, and domain configuration.

```bash
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh check --domain my_area
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

Only HRRR is available through WindNinja's native `run --mode reanalysis`
pastcast system. NBM remains available for native forecast runs through
`./deploy/gcp/mwn.sh run --model NBM`.

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
| `--model HRRR\|NBM\|NAM\|RAP\|GFS` | Weather model; reanalysis currently supports HRRR only | `HRRR` |
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

Point-sampling note:
WindNinja rejects `input_points_file` when `momentum_flag = true`. For Synoptic validation on momentum runs, run the simulation without `--points-file` and use `validate-rasters` afterward.

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
- `PASTCAST-*_*_vel.asc` / `*_ang.asc` -- parent weather-model rasters written by WindNinja
- `*_sample_points.csv` -- sampled WindNinja and raw weather-model vectors at requested station points, only when `--points-file` is used and WindNinja accepts that configuration

Without `--keep-temp`, only archive zip is kept in `runtime/archives/`, but it still contains these run outputs.

## run-grid

Run one WindNinja timestep from prepared AAIGrid speed and direction grids. This
does not download a weather model and does not change the normal `run` workflow.

```bash
./deploy/gcp/mwn.sh run-grid \
  --speed-grid runtime/forcing/case/speed.asc \
  --direction-grid runtime/forcing/case/direction.asc \
  --time 202601010000 \
  --domain my_area
```

Required:

| Flag | Description |
|------|-------------|
| `--speed-grid PATH` | Wind speed AAIGrid in meters per second |
| `--direction-grid PATH` | Meteorological wind direction AAIGrid in degrees |
| `--time UTC` | Timestep time (`YYYYMMDDHHMM` or `YYYY-MM-DDTHH:MM`) |
| `--domain KEY` | DEM-backed domain key from `config/domains.json` |

Optional:

| Flag | Description |
|------|-------------|
| `--height N` | Output height above ground in meters, default `10` |
| `--label NAME` | Output/archive label, default `external` |
| `--keep-temp` | Keep raw output in `runtime/temp/` |
| `--no-upload` | Skip GCS upload |
| `--dry-run` | Validate inputs and write the WindNinja config only |

`run-grid` validates that both grids exist, have matching dimensions and CRS,
match the terrain CRS, fully cover the terrain, and have no no-data cells over
the domain. Gridded forcing requires a DEM. If a domain points at `name.lcp`
and `static_data/name.tif` exists, the command uses the `.tif` automatically.

Output directories use:

```text
runtime/temp/{domain}_{YYYYMMDD_HHMM}_grid_{label}
runtime/archives/{domain}_grid_{label}_{YYYYMMDD_HHMM}.zip
```

## forcing-from-grib

Convert one local GRIB/NetCDF timestep with U/V wind components or direct
speed/direction fields into WindNinja-ready `speed.asc` and `direction.asc`
files.

```bash
./deploy/gcp/mwn.sh forcing-from-grib runtime/forcing/raw/input.grib2 \
  --domain my_area \
  --time 202601010000 \
  --u-var UGRD \
  --v-var VGRD \
  --level 10m \
  --out runtime/forcing/case
```

Inputs and output directories must be under mounted repo paths, normally
`runtime/forcing/...`. The command uses `gdalinfo -json` to select matching
bands or subdatasets. If selection is ambiguous, rerun with exact GDAL dataset
overrides:

```bash
./deploy/gcp/mwn.sh forcing-from-grib runtime/forcing/raw/input.nc \
  --domain my_area --time 202601010000 --u-var u --v-var v --level 10m \
  --u-source 'NETCDF:"runtime/forcing/raw/input.nc":u10' \
  --v-source 'NETCDF:"runtime/forcing/raw/input.nc":v10' \
  --out runtime/forcing/case
```

The conversion reprojects U and V onto the domain DEM grid, then computes:

```text
speed = sqrt(u*u + v*v)
direction = (270 - atan2(v, u) * 180/pi) % 360
```

It writes `speed.asc`, `direction.asc`, matching `.prj` files, and
`metadata.json`.

For sources such as NBM that already publish speed and direction:

```bash
./deploy/gcp/mwn.sh forcing-from-grib runtime/forcing/raw/input.grib2 \
  --domain berthoud_pass \
  --time 202601010100 \
  --speed-var WIND \
  --direction-var WDIR \
  --speed-units mps \
  --level 10m \
  --out runtime/forcing/case
```

## smoke

Run a fast domain-average test using fixed wind input (`10 mph` from `270` degrees), one forecast hour, and no upload. This uses the same preflight path as `run`.

```bash
./deploy/gcp/mwn.sh smoke
./deploy/gcp/mwn.sh smoke --domain my_area
./deploy/gcp/mwn.sh smoke --domain my_area --keep-temp
```

`--keep-temp` leaves raw output under `runtime/temp/` for debugging instead of
archiving and deleting the run directory.

## shell

Open an interactive bash shell inside the container. Useful for manual WindNinja runs, inspecting files, or debugging.

```bash
./deploy/gcp/mwn.sh shell

# Inside the container, you can run WindNinja directly:
source /opt/openfoam9/etc/bashrc
WindNinja_cli my_config.cfg
```

## domain create

Download terrain, register a domain in `config/domains.json`, set it as the
default, and run preflight.

```bash
./deploy/gcp/mwn.sh domain create my_area \
  --bbox 39.65 -106.0 39.55 -106.15 \
  --terrain-source us \
  --label "My Area"
```

Required:

| Option | Description |
|--------|-------------|
| `KEY` | Domain key to register |
| `--bbox NORTH EAST SOUTH WEST` | Bounding box in decimal degrees |
| `--terrain-source us\|srtm\|gmted\|lcp` | Terrain source |

Optional:

| Option | Description |
|--------|-------------|
| `--label TEXT` | Human-readable label |
| `--resolution N` | DEM resolution in meters for `us`, `srtm`, or `gmted` |
| `--output PATH` | Terrain output path |
| `--no-set-default` | Register without changing `default_domain` or `MWN_DOMAIN_ID` |
| `--no-check` | Skip the post-registration preflight check |

Default output is `MWN_STATIC_DATA_ROOT/KEY.tif` for DEM sources and
`MWN_STATIC_DATA_ROOT/KEY.lcp` for LCP. Use `--output` to override it.

Examples:

```bash
./deploy/gcp/mwn.sh domain create keystone \
  --bbox 39.65 -106.0 39.55 -106.15 \
  --terrain-source us \
  --resolution 10

./deploy/gcp/mwn.sh domain create alps \
  --bbox 45.5 7.0 45.0 6.5 \
  --terrain-source srtm \
  --resolution 30 \
  --no-check

./deploy/gcp/mwn.sh domain create forested_area \
  --bbox 40.0 -105.0 39.5 -105.5 \
  --terrain-source lcp
```

## fetch-terrain

Download both DEM and LCP terrain for one domain. The DEM is saved first as a
fallback. The LCP is saved and registered last, so the active domain uses LCP
when both downloads succeed.

```bash
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"

./deploy/gcp/mwn.sh fetch-terrain --area-file keystone.kml --padding-km 1 \
  --domain keystone \
  --label "Keystone"

./deploy/gcp/mwn.sh fetch-terrain 39.65 -106.0 39.55 -106.15 \
  --domain keystone
```

Key options:

| Flag | Description |
|------|-------------|
| `--domain KEY` | Required. Add/update `KEY` in `config/domains.json` |
| `--label TEXT` | Human-readable domain label |
| `--no-set-default` | Register without changing the default domain |
| `--dem-source us\|srtm\|gmted` | DEM source, default `us` |
| `--dem-resolution N` | DEM output resolution in meters |
| `--dem-output PATH` | DEM output path, default `MWN_STATIC_DATA_ROOT/KEY.tif` |
| `--lcp-output PATH` | LCP output path, default `MWN_STATIC_DATA_ROOT/KEY.lcp` |

Area inputs match `fetch-dem`: center/size, KML/KMZ area file, or explicit bbox.

## fetch-dem

Download a DEM (Digital Elevation Model) into `static_data/`. Add `--domain`
to register the terrain immediately and make it the default domain.

```bash
# Beginner path: default output is static_data/keystone.tif
./deploy/gcp/mwn.sh fetch-dem --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"

# Area file path; bbox is computed from KML/KMZ coordinates
./deploy/gcp/mwn.sh fetch-dem --area-file keystone.kml --padding-km 1 \
  --domain keystone \
  --label "Keystone"

# Custom output path
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/keystone.tif

# Explicit source/resolution and domain registration
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/keystone.tif us 10 \
  --domain keystone --label "Keystone"

# Backwards-compatible advanced registration without changing the default
./deploy/gcp/mwn.sh fetch-dem 45.5 7.0 45.0 6.5 static_data/alps.tif srtm 30 \
  --register-domain alps --label "Alps"
```

Area inputs:

| Input | Description |
|-------|-------------|
| `--center LAT LON --size-km N` | Build an `N x N` km square around one point |
| `--center LAT LON --radius-km N` | Build a box from a radius; equivalent to `--size-km 2N` |
| `--area-file PATH` | Read KML/KMZ coordinates and use their bbox |
| `<north> <east> <south> <west>` | Explicit bbox in decimal degrees |

Optional positional arguments after area input: `[output] [source] [resolution]`.
With explicit bbox: `<north> <east> <south> <west> [output] [source] [resolution]`.

Sources: `us` for USGS 3DEP 10m in the US, `srtm` for global 30m OpenTopography SRTM, and `gmted` for coarse global GMTED2010.

You can find center coordinates using Google Maps by right-clicking a point.
Center/size boxes are limited to 50 km; use explicit bbox for larger advanced
domains.

**Note:** SRTM downloads require an OpenTopography API key. Get a free one at [opentopography.org](https://opentopography.org/) and set it in `config/runtime.env`:

```
CUSTOM_SRTM_API_KEY=your_key_here
```

Optional registration flags:

| Flag | Description |
|------|-------------|
| `--domain KEY` | Add or update `KEY`, set it as default, and update `MWN_DOMAIN_ID` |
| `--register-domain KEY` | Add or update `KEY` without changing the default unless `--set-default` is passed |
| `--label TEXT` | Human-readable domain label |
| `--set-default` | Set `default_domain` and `MWN_DOMAIN_ID` to `KEY` |
| `--no-set-default` | Register without changing the default domain |
| `--padding-km N` | Expand a KML/KMZ area-file bbox |

Bounding boxes are `N E S W` in decimal degrees. The script rejects swapped
coordinates: `north` must be greater than `south`, and `east` must be greater
than `west`. For Colorado, `east=-106.0` and `west=-106.15` is valid.

## fetch-lcp

Download an LCP (Landscape) file from LANDFIRE. LCP files include elevation plus
7 additional bands of vegetation/fuel data, giving WindNinja much more accurate
results than a bare DEM. Only available for the United States.

```bash
# Download LCP for an area
./deploy/gcp/mwn.sh fetch-lcp --center 39.60 -106.08 --size-km 12

# Register it with default output static_data/keystone.lcp
./deploy/gcp/mwn.sh fetch-lcp --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"

# Use a KML/KMZ area file
./deploy/gcp/mwn.sh fetch-lcp --area-file keystone.kml --padding-km 1 \
  --domain keystone \
  --label "Keystone"

# Custom output path
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/keystone.lcp

# Backwards-compatible advanced registration
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/keystone.lcp \
  --register-domain keystone --label "Keystone" --set-default
```

Area inputs match `fetch-dem`. Optional positional argument after area input:
`[output_path]`.

LANDFIRE downloads can take several minutes because the server processes your request. The download includes elevation, slope, aspect, fire behavior fuel model, canopy cover, canopy height, crown base height, and crown bulk density.

Registration flags match `fetch-dem`: prefer `--domain KEY`; use
`--register-domain KEY` when you do not want to change the default domain.

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
  --bbox-output runtime/validation/loveland_bbox.json \
  --start 202601010000 --end 202601080000
```

The station manifest is a CSV with `station_id,label,group,height_m_override`. If `height_m_override` is empty, the command uses the Synoptic wind sensor `position` metadata. It also prints a suggested padded bbox for the station set.

The generated metadata JSON is used by both `validate` and `validate-rasters`. If one or more stations lack usable sensor-height metadata, pass `--default-height 10` (or another justified fallback height) to keep the prep step moving.

Requires `MWN_SYNOPTIC_TOKEN` (or `CUSTOM_API_KEY`) in `config/runtime.env`, unless you pass `--token`.
The token also needs account-level access to Synoptic weather data. A syntactically valid token with insufficient account permissions will still fail.

## validate

Compare a WindNinja `output_points_file` CSV against Synoptic observations and the parent HRRR vectors sampled at the same points.

Use this command only when WindNinja successfully produces `output_points_file`. It is not the recommended path for momentum runs because upstream WindNinja rejects `input_points_file` with `momentum_flag = true`.

```bash
./deploy/gcp/mwn.sh run --mode reanalysis \
  --start 202601010000 --end 202601080000 \
  --model HRRR \
  --domain summit_county \
  --points-file runtime/validation/loveland_points.csv \
  --keep-temp --no-upload

./deploy/gcp/mwn.sh validate \
  --points-output runtime/temp/summit_county_20260101_0000_reanalysis_168h_HRRR/summit_county_sample_points.csv \
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

## validate-rasters

Compare nearest WindNinja raster cells and nearest parent-HRRR raster cells against Synoptic observations for all matched station-hours in a completed run directory.

Recommended use:

- momentum solver validation
- historical HRRR-vs-WindNinja studies
- spot-instance workflows where you keep completed run directories and validate afterward

```bash
./deploy/gcp/mwn.sh validate-rasters \
  --run-dir runtime/temp/summit_county_20260101_0000_reanalysis_3h_HRRR \
  --metadata-file runtime/validation/loveland_metadata.json \
  --start 202601010000 --end 202601010300 \
  --samples-csv runtime/validation/loveland_3h_raster_samples.csv \
  --station-summary-csv runtime/validation/loveland_3h_raster_station_summary.csv \
  --group-summary-csv runtime/validation/loveland_3h_raster_group_summary.csv \
  --summary-json runtime/validation/loveland_3h_raster_summary.json
```

Outputs:
- `samples.csv` with one row per matched station-hour
- `station_summary.csv` with per-station MAE/RMSE
- `group_summary.csv` with grouped MAE/RMSE
- `summary.json` with overall metrics and WindNinja-vs-HRRR improvement deltas

This command does not feed Synoptic into WindNinja. HRRR still drives the simulation. Synoptic only supplies station coordinates/heights and observed wind for the comparison.

## validate-study

Run a chunked validation study from `config/studies/<name>.json`. This is the
recommended path for Berthoud Pass and other long-period comparisons.

```bash
# Inspect planned chunks and output paths only
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3 \
  --plan

# Run a 3-hour pilot
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3

# Run a 7-day study in 24-hour chunks
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --end 202601080000 \
  --chunk-hours 24

```

The command prepares Synoptic metadata for the configured station manifest,
runs model chunks, validates completed rasters, and writes:

- `runtime/validation/berthoud_pass/stations.csv`
- `runtime/validation/berthoud_pass/station_metadata.json`
- `runtime/validation/berthoud_pass/chunks/*/samples.csv`
- `runtime/validation/berthoud_pass/samples.csv`
- `runtime/validation/berthoud_pass/station_summary.csv`
- `runtime/validation/berthoud_pass/group_summary.csv`
- `runtime/validation/berthoud_pass/summary.json`

Useful flags:

- `--plan`: print chunk/run paths and exit
- `--dry-run`: print commands without running them
- `--force`: rerun completed chunks and validations
- `--skip-runs`: validate existing run directories only
- `--model HRRR`: override the study model; historical validation is HRRR only

Requires `MWN_SYNOPTIC_TOKEN` with Synoptic weather-data access. Station
selection is explicit: edit `config/stations/berthoud_pass_validation_manifest.csv`
to choose K0CO or any other stations to compare.

Historical `validate-study` runs are HRRR only because WindNinja exposes native
HRRR pastcast but not native NBM pastcast.

`MWN_NUM_THREADS=N` overrides `num_threads` for generated WindNinja configs in
`mwn.sh run`, `run-grid`, and `validate-study`. Keep this at or below physical
CPU cores for OpenFOAM momentum runs; on the current 6-physical / 12-logical CPU
machine, use `MWN_NUM_THREADS=6` for the high-thread trial.

## plot-validation

Build static SVG plots and an HTML index from completed validation samples. This
command runs on the host with the Python standard library only, so it can be
rerun while a long validation container is still processing future chunks.

```bash
./deploy/gcp/mwn.sh plot-validation \
  --study-root runtime/validation/berthoud_pass \
  --title "Berthoud Pass Validation - January 2026"
```

By default it reads `chunks/*/samples.csv`, deduplicates overlapping station
hours, and writes:

- `runtime/validation/berthoud_pass/plots/index.html`
- `runtime/validation/berthoud_pass/plots/speed_timeseries.svg`
- `runtime/validation/berthoud_pass/plots/speed_error_timeseries.svg`
- `runtime/validation/berthoud_pass/plots/direction_error_timeseries.svg`
- `runtime/validation/berthoud_pass/plots/speed_scatter.svg`
- `runtime/validation/berthoud_pass/plots/daily_metrics.svg`
- `runtime/validation/berthoud_pass/plots/sampling_map_<station>.png` when station metadata and rasters exist
- `runtime/validation/berthoud_pass/plots/station_metrics.csv`
- `runtime/validation/berthoud_pass/plots/plot_summary.json`

Useful flags:

- `--samples-csv PATH`: plot one specific samples CSV; can be passed more than once
- `--output-dir PATH`: override the output directory
- `--station-id ID`: plot one station from a multi-station validation
- `--speed-units mph|mps|kph|kts`: label plot units

## schedule

Start the automatic scheduler. By default it runs an 18-hour HRRR forecast at :15 past every hour.

```bash
./deploy/gcp/mwn.sh schedule
```

The scheduler runs as a background Docker container. It persists across reboots.

Configure it in `config/runtime.env`:

| Setting | Default |
|---------|---------|
| `MWN_SCHEDULE_MINUTE` | `15` |
| `MWN_SCHEDULE_MODE` | `forecast` |
| `MWN_SCHEDULE_MODEL` | `HRRR` |
| `MWN_SCHEDULE_HOURS` | `18` |

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

All settings are in `config/runtime.env`. Create it with:

```bash
./deploy/gcp/mwn.sh init
```

| Setting | Description | Default |
|---------|-------------|---------|
| `MWN_DOMAIN_ID` | Active domain (must match key in `domains.json`) | `my_area` |
| `MWN_DOCKER_IMAGE` | Docker image used by Compose | `mountain-windninja:local` |
| `MWN_RUNTIME_ROOT` | Where output files go | `runtime` |
| `MWN_STATIC_DATA_ROOT` | Where terrain files live; update Compose mounts if changed | `static_data` |
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
- `elevation_file` -- terrain filename (looked up under `MWN_STATIC_DATA_ROOT`)

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

For Point initialization, open a shell (`mwn.sh shell`) and write a custom
`.cfg` file. For Gridded initialization, use `run-grid` and optionally
`forcing-from-grib`.

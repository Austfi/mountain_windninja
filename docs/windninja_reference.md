# WindNinja Official Reference

This document captures key information from the official WindNinja repository, wiki,
and documentation. It serves as a context reference for anyone working on this project.

Sources:
- [firelab/windninja GitHub](https://github.com/firelab/windninja)
- [WindNinja Wiki](https://github.com/firelab/windninja/wiki)
- [WindNinja USFS Page](https://research.fs.usda.gov/firelab/products/dataandtools/windninja)
- [Official Dockerfile](https://github.com/firelab/windninja/blob/master/Dockerfile)

---

## WindNinja Overview

WindNinja is a diagnostic wind model developed by the USDA Forest Service, Missoula
Fire Sciences Laboratory. It computes spatially varying wind fields for wildland fire
and other applications. Current version: **3.12.2** (March 2026).

WindNinja has two solvers:
1. **Conservation of Mass** (default) -- fast, runs in seconds
2. **Conservation of Mass + Momentum** (OpenFOAM-based) -- slower, more accurate in
   complex terrain. Requires `momentum_flag = true` and OpenFOAM installed.

## Initialization Methods

WindNinja supports four initialization methods. The `initialization_method` config
option must be set to exactly one of these:

### 1. domainAverageInitialization

Uniform wind across the entire domain. User specifies one speed and direction.
WindNinja adjusts for terrain effects. No internet connection needed.

Required config options:
```
initialization_method = domainAverageInitialization
input_speed = 10.0
input_speed_units = mph          # mph, mps, kph, kts
input_direction = 270            # degrees, 0=N, 90=E, 180=S, 270=W
input_wind_height = 10.0
units_input_wind_height = m
```

### 2. pointInitialization

Uses weather station observations. WindNinja interpolates between station locations
and adjusts for terrain. Can automatically fetch stations from the Synoptic (formerly
MesoWest/Mesonet) API.

Required config options:
```
initialization_method = pointInitialization
match_points = true
fetch_type = stid                     # or: latlon, box, point
wx_station_filename = path/to/stations.csv
```

Station CSV format (for manual stations):
```
Station_Name,Coord_Sys(PROJCS or GEOGCS),Datum,Lat/YCoord,Lon/XCoord,Height,Height_Units(meters or feet),Speed,Speed_Units(mph mps kph or kts),Direction(degrees),Temperature,Temperature_Units(F C or K),Cloud_Cover(%),Radius_of_Influence,Radius_of_Influence_Units(miles meters or km),date_time
```

Automatic station fetching uses the Synoptic API. An API key can be set with
`CUSTOM_API_KEY` environment variable. WindNinja fetches RAWS and NWS/FAA stations
within the DEM bounds. There is a hard limit of 100,000 station-hours.

### 3. wxModelInitialization

Downloads NWS weather model data from NOMADS or UCAR and downscales it to the terrain.
This is the primary mode for operational forecasting.

Required config options:
```
initialization_method = wxModelInitialization
wx_model_type = NOMADS-HRRR-CONUS-3-KM
forecast_duration = 18
time_zone = America/Denver           # or UTC, auto-detect
```

Alternatively, use a pre-downloaded forecast file:
```
initialization_method = wxModelInitialization
forecast_filename = /path/to/forecast.nc
time_zone = America/Denver
```

### 4. griddedInitialization (CLI only)

Supply your own wind speed and direction grids. Grids must be AAIGrid (.asc) format,
same projection as the DEM, and completely overlap the DEM.

Required config options:
```
initialization_method = griddedInitialization
input_speed_grid = /path/to/speed.asc
input_dir_grid = /path/to/direction.asc
input_speed_units = mps
```

Note: This method does not support multiple time steps in a single run. Each time step
requires a separate run. Diurnal winds require additional settings: `uni_air_temp`,
`air_temp_units`, `uni_cloud_cover`, `cloud_cover_units`, plus date/time fields.

---

## Available Weather Models

### NOMADS Models (current, recommended)

| Model | WindNinja Name | Resolution | Domain | Max Forecast |
|-------|---------------|-----------|--------|-------------|
| HRRR | `NOMADS-HRRR-CONUS-3-KM` | 3 km | CONUS | 18h (48h extended) |
| NBM | `NOMADS-NBM-CONUS-2.5-KM` | 2.5 km | CONUS | 264h |
| NAM Nest | `NOMADS-NAM-NEST-CONUS-3-KM` | 3 km | CONUS | 60h |
| NAM CONUS | `NOMADS-NAM-CONUS-12-KM` | 12 km | CONUS | 84h |
| NAM Alaska | `NOMADS-NAM-ALASKA-11.25-KM` | 11.25 km | Alaska | 84h |
| NAM NA | `NOMADS-NAM-NORTH-AMERICA-32-KM` | 32 km | North America | 84h |
| RAP | `NOMADS-RAP-CONUS-13-KM` | 13 km | CONUS | 21h |
| RAP NA | `NOMADS-RAP-NORTH-AMERICA-32-KM` | 32 km | North America | 21h |
| GFS | `NOMADS-GFS-GLOBAL-0.25-DEG` | ~25 km | Global | 384h |
| HIRES ARW | `NOMADS-HIRES-ARW-CONUS-5-KM` | ~5 km | CONUS | varies |
| HIRES NMM | `NOMADS-HIRES-NMM-CONUS-5-KM` | ~5 km | CONUS | varies |
| HIRES Alaska | `NOMADS-HIRES-ALASKA-5-KM` | ~5 km | Alaska | varies |

### UCAR/THREDDS Models (legacy, may be less reliable)

| Model | WindNinja Name | Resolution | Domain |
|-------|---------------|-----------|--------|
| NAM | `UCAR-NAM-CONUS-12-KM` | 12 km | CONUS |
| NAM Alaska | `UCAR-NAM-ALASKA-11-KM` | 11 km | Alaska |
| NDFD | `UCAR-NDFD-CONUS-2.5-KM` | 2.5 km | CONUS |
| RAP | `UCAR-RAP-CONUS-13-KM` | 13 km | CONUS |
| GFS | `UCAR-GFS-GLOBAL-0.5-DEG` | 0.5° | Global |

### Pastcast (Historical)

| Model | WindNinja Name | Resolution | Domain |
|-------|---------------|-----------|--------|
| HRRR Archive | `PASTCAST-GCP-HRRR-CONUS-3-KM` | 3 km | CONUS |

Uses archived HRRR data from Google Cloud Platform. Available from 2014 to present.
Added in WindNinja 3.12.0.

---

## Elevation/Terrain Data

WindNinja accepts these elevation file formats: `.asc` (AAIGrid), `.tif` (GeoTIFF),
`.img` (ERDAS Imagine), `.lcp` (LANDFIRE Landscape).

### Requirements
- **Projected coordinate system** (UTM recommended). Geographic lat/lon will fail.
- **North-up orientation** (critical -- non-north-up DEMs cause incorrect results).
- **Elevation in meters**
- **No-data values** will cause errors unless `NINJA_FILL_DEM_NO_DATA` is set.

### Built-in DEM Download (fetch_dem)

WindNinja includes `fetch_dem` for downloading elevation data:

```
fetch_dem [--bbox north east south west]
          [--point x y x_buf y_buf]
          [--ogr_ds filename]
          [--out_res res]
          [--src us/world/gmted/lcp]
          [--fill_no_data]
          output_file
```

Sources:
- `us` -- USGS SRTM via OpenTopography (requires `CUSTOM_SRTM_API_KEY`)
- `world` -- Global SRTM via OpenTopography
- `gmted` -- GMTED2010 (coarser global data)
- `lcp` -- LANDFIRE LCP (US only, includes vegetation data)

The `--src lcp` option downloads from LANDFIRE and can take several minutes due
to server processing. The LANDFIRE server has configurable timeouts:
- `LCP_DOWNLOAD_WAIT` -- seconds between retry attempts (default varies)
- `LCP_DOWNLOAD_TRIES` -- number of retries before giving up

### External DEM Sources
- **USGS 3DEP / National Map**: https://apps.nationalmap.gov/downloader/
  - 1/3 arc-second (10m) and 1 arc-second (30m) for US
- **OpenTopography**: https://opentopography.org/
  - SRTM (30m), COP30 (30m global), NASADEM, ALOS World 3D
  - Free API key required: set `CUSTOM_SRTM_API_KEY`
- **LANDFIRE**: https://www.landfire.gov/viewer/
  - LCP landscape files with vegetation data (US only)
  - LFPS REST API: https://lfps.usgs.gov
  - Python package: `pip install landfire`

### LCP (Landscape) Files

As of 2024, LANDFIRE is transitioning from `.lcp` to GeoTIFF landscape files.
An LCP contains 8 bands:
1. Elevation
2. Slope (degrees)
3. Aspect (degrees)
4. Fire Behavior Fuel Model (Anderson 13 or Scott & Burgan 40)
5. Canopy Cover (%)
6. Canopy Height
7. Crown Base Height
8. Crown Bulk Density

When using an LCP, WindNinja reads vegetation data directly from the file.
The `vegetation` config option is ignored.

---

## Configuration File Reference

### Core Settings

| Option | Values | Description |
|--------|--------|-------------|
| `num_threads` | integer (default: 1) | CPU threads for simulation |
| `elevation_file` | path | Input elevation file (*.asc, *.lcp, *.tif, *.img) |
| `initialization_method` | string | See "Initialization Methods" above |
| `time_zone` | string | IANA time zone or `auto-detect` or `UTC` |
| `vegetation` | `grass`, `brush`, `trees` | Uniform vegetation (ignored for LCP) |

### Wind Input (domainAverageInitialization)

| Option | Values | Description |
|--------|--------|-------------|
| `input_speed` | float | Wind speed |
| `input_speed_units` | `mph`, `mps`, `kph`, `kts` | Speed units |
| `input_direction` | 0-360 | Wind direction (degrees, 0=N) |
| `input_wind_height` | float | Height of input wind above ground |
| `units_input_wind_height` | `m`, `ft` | Units for input height |

### Weather Model (wxModelInitialization)

| Option | Values | Description |
|--------|--------|-------------|
| `wx_model_type` | string | Model name (see weather models table) |
| `forecast_duration` | integer | Hours to forecast |
| `forecast_filename` | path | Pre-downloaded forecast file |
| `forecast_time` | UTC string | Specific time(s) to simulate |

### Date/Time (for domain-average and point runs)

| Option | Values | Description |
|--------|--------|-------------|
| `year`, `month`, `day` | integers | Simulation date |
| `hour`, `minute` | integers | Simulation time (in `time_zone`) |
| `start_year` .. `stop_minute` | integers | Start/stop for multi-step runs |

### Solver

| Option | Values | Description |
|--------|--------|-------------|
| `momentum_flag` | `true`, `false` | Use OpenFOAM momentum solver |
| `number_of_iterations` | integer (default: 300) | OpenFOAM iterations |
| `mesh_resolution` | float | Grid cell size |
| `units_mesh_resolution` | `m`, `km` | Mesh resolution units |
| `non_neutral_stability` | `true`, `false` | Non-neutral atmospheric stability |

### Diurnal Winds

| Option | Values | Description |
|--------|--------|-------------|
| `diurnal_winds` | `true`, `false` | Enable thermal slope winds |
| `uni_air_temp` | float | Air temperature (for diurnal calculation) |
| `air_temp_units` | `C`, `F`, `K` | Temperature units |
| `uni_cloud_cover` | 0-100 or 0.0-1.0 | Cloud cover |
| `cloud_cover_units` | `percent`, `fraction` | Cloud cover units |

### Output

| Option | Values | Description |
|--------|--------|-------------|
| `output_wind_height` | float (default: 10.0) | Output wind height above ground |
| `units_output_wind_height` | `m`, `ft` | Output height units |
| `output_speed_units` | `mph`, `mps`, `kph`, `kts` | Output speed units |
| `output_path` | path | Directory for output files |
| `write_goog_output` | `true`, `false` | Write KMZ for Google Earth |
| `write_ascii_output` | `true`, `false` | Write ASCII grid files |
| `write_shapefile_output` | `true`, `false` | Write shapefiles |
| `write_pdf_output` | `true`, `false` | Write PDF maps |
| `write_vtk_output` | `true`, `false` | Write VTK 3D output |
| `write_farsite_atm` | `true`, `false` | Write FARSITE atmosphere file |
| `write_wx_model_goog_output` | `true`, `false` | Write KMZ of raw wx model wind |
| `write_wx_model_shapefile_output` | `true`, `false` | Write shapefile of raw wx model |
| `write_wx_model_ascii_output` | `true`, `false` | Write ASCII of raw wx model |
| `goog_out_resolution` | float (-1 = auto) | KMZ output resolution |
| `units_goog_out_resolution` | `m` | KMZ resolution units |
| `ascii_out_resolution` | float (-1 = auto) | ASCII output resolution |
| `units_ascii_out_resolution` | `m` | ASCII resolution units |
| `goog_out_use_consistent_color_scale` | `true`, `false` | Consistent colors across timesteps |

### Point Sampling

Extract wind predictions at specific locations:

| Option | Values | Description |
|--------|--------|-------------|
| `input_points_file` | path | CSV with lat,lon,height points |
| `output_points_file` | path | Output CSV (optional) |

Input CSV format:
```
WGS84
point_name,latitude,longitude,height_meters_above_ground
```

Output CSV columns: `ID,lat,lon,height,datetime,u,v,w,wx_u,wx_v`

---

## Environment Variables

### General
| Variable | Description |
|----------|-------------|
| `WINDNINJA_DATA` | WindNinja data directory path |
| `CUSTOM_SRTM_API_KEY` | OpenTopography API key for DEM downloads |
| `NINJA_FILL_DEM_NO_DATA` | Auto-fill no-data values (overwrites DEM) |
| `NINJA_DISABLE_CALL_HOME` | Disable update check |

### OpenFOAM / Momentum Solver
| Variable | Description |
|----------|-------------|
| `CPL_DEBUG=NINJAFOAM` | Enable OpenFOAM debug output |
| `NINJAFOAM_ITERATIONS` | Override solver iterations |
| `NINJAFOAM_MESH_COUNT` | Override mesh count |
| `NINJAFOAM_KEEP_ALL_TIMESTEPS` | Keep OpenFOAM case files for all timesteps |
| `WRITE_TURBULENCE` | Write turbulence output to KMZ |
| `FOAM_USER_LIBBIN` | OpenFOAM user library path (set to `/usr/local/lib/` in Docker) |

### NOMADS Weather Downloads
| Variable | Default | Description |
|----------|---------|-------------|
| `NOMADS_THREAD_COUNT` | 4 | Download threads |
| `NOMADS_MAX_FCST_REWIND` | 2 | Forecast cycles to try |
| `NOMADS_HTTP_TIMEOUT` | 20 | HTTP timeout (seconds) |

### LANDFIRE / LCP Downloads
| Variable | Description |
|----------|-------------|
| `LCP_DOWNLOAD_WAIT` | Seconds between LANDFIRE retry attempts |
| `LCP_DOWNLOAD_TRIES` | Number of retries before giving up |
| `LCP_KEEP_ARCHIVE` | `ON` to keep downloaded zip archive |

### Point Initialization / Station Fetch
| Variable | Description |
|----------|-------------|
| `CUSTOM_API_KEY` | Custom Synoptic/Mesonet API token |
| `NINJA_POINT_MATCH_OUT_RELAX` | Outer loop relaxation |
| `NINJA_POINT_MAX_MATCH_ITERS` | Maximum matching iterations |
| `NINJA_POINT_INV_DIST_WEIGHT` | Inverse distance weighting exponent |

---

## Docker Build (Official)

The official Dockerfile (from `firelab/windninja/Dockerfile`) uses Ubuntu 20.04 and
OpenFOAM 8. Our project Dockerfile follows the
[22.04 wiki build instructions](https://github.com/firelab/windninja/wiki/Building-WindNinja-on-Linux-22.04):
- Base: `ubuntu:22.04`
- CMake flags:
  - `-D SUPRESS_WARNINGS=ON`
  - `-D NINJAFOAM=ON` (momentum solver)
  - `-D BUILD_FETCH_DEM=ON` (DEM downloader)
  - `-D BUILD_SLOPE_ASPECT_GRID=ON`
  - `-D BUILD_FLOW_SEPARATION_GRID=ON`
  - `-D NINJA_GUI=OFF` (CLI only)
- OpenFOAM 9 from `dl.openfoam.org/ubuntu`
- Poppler 22.02.0, PROJ 8.2.1, GDAL 3.4.1

### Critical OpenFOAM Environment Setup

When running WindNinja with the momentum solver inside Docker or Singularity, these
environment variables **must** be set before calling `WindNinja_cli`:

```bash
source /opt/openfoam9/etc/bashrc
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
export FOAM_USER_LIBBIN=/usr/local/lib/
```

For HPC/Slurm: SLURM environment variables conflict with OpenFOAM. They must be
temporarily unset before running WindNinja:
```bash
SLURM_ENV=$(env | grep ^SLURM_)
unset $(env | grep ^SLURM_ | cut -d= -f1)
WindNinja_cli config.cfg
export ${SLURM_ENV}
```

### Our Dockerfile vs Official

Our Dockerfile (`/Dockerfile`) follows the 22.04 wiki instructions with these additions:
- Python venv with project dependencies (google-cloud-storage, python-dotenv, pytest)
- Cron support for scheduled forecasts
- Project files copied into `/opt/mountain_windninja`
- Poppler, PROJ, and GDAL built from source (per `build_deps_ubuntu_2204.sh`)
- OpenFOAM 9 sourced in `/etc/bash.bashrc` for all shells

---

## Common Pitfalls

### DEM Issues
- **Geographic CRS fails**: WindNinja requires projected CRS (UTM, etc.), not EPSG:4326.
  Use `gdalwarp -t_srs EPSG:326XX input.tif output.tif` to reproject.
- **Non-north-up DEM**: Causes silently incorrect results. Check with `gdalinfo`.
- **No-data holes**: Cause solver failures. Set `NINJA_FILL_DEM_NO_DATA=ON` to auto-fill
  (overwrites input file).
- **Too large domain**: Momentum solver memory scales with cell count. Keep under ~50x50 km
  at 80m resolution on a 16GB VM.

### OpenFOAM Issues
- **NINJAFOAM case not cleaned up**: Stale `NINJAFOAM_*` directories from failed runs can
  cause subsequent runs to fail. Delete them before retrying.
- **OpenFOAM not sourced**: If `momentum_flag = true` but OpenFOAM bashrc isn't sourced,
  WindNinja will fail with cryptic errors about missing commands.
- **Root execution**: OpenMPI refuses to run as root by default. Must set
  `OMPI_ALLOW_RUN_AS_ROOT=1` and `OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1`.

### Weather Model Issues
- **NOMADS server down**: NOAA servers have occasional outages. The `NOMADS_MAX_FCST_REWIND`
  variable controls how many past forecast cycles to try (default 2).
- **HRRR forecast only 18h**: HRRR standard forecasts are 18h. Extended HRRR goes to 48h
  but only runs every 6 hours (00, 06, 12, 18 UTC).
- **NBM not found**: NBM was added in WindNinja 3.12.0. Older versions don't support it.
- **Time zone mismatch**: Forecast times in the output use the `time_zone` setting. Use
  `UTC` for consistency or `auto-detect` to use the DEM's geographic location.

### Output Issues
- **goog_out_use_consistent_color_scale**: Can cause SIGSEGV on some multi-hour runs.
  Set to `false` if experiencing crashes.
- **ASCII output resolution -1**: Auto-matches mesh resolution. Set explicitly if you need
  a specific output resolution.

---

## Useful CLI Commands

```bash
# Print version
WindNinja_cli --version

# Print all available config options
WindNinja_cli --runtime_options

# Print citation info
WindNinja_cli --citation

# Run from config file
WindNinja_cli my_config.cfg

# Run with inline options (no config file)
WindNinja_cli --num_threads=4 --elevation_file=dem.tif \
  --initialization_method=domainAverageInitialization \
  --input_speed=10 --input_speed_units=mph \
  --input_direction=270 --input_wind_height=10 \
  --units_input_wind_height=m --vegetation=trees \
  --mesh_resolution=100 --units_mesh_resolution=m \
  --output_speed_units=mph --write_goog_output=true \
  --write_ascii_output=true

# Download a DEM
fetch_dem --bbox 40.0 -105.0 39.5 -105.5 --src us --out_res 30 output.tif
```

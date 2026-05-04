# AGENTS.md -- Mountain WindNinja

Context for AI agents working on this repo.

## What This Is

A Docker-based CLI wrapper around [WindNinja](https://github.com/firelab/windninja) for running wind simulations on cloud VMs (primarily GCP). The user interacts via `./deploy/gcp/mwn.sh <command>`.

## Architecture

```
Host (GCP VM or any Linux box)
  └── mwn.sh ← user-facing CLI, runs on host
        └── docker compose run --rm shell ← spins up container per command
              └── mountain-windninja:local image
                    ├── WindNinja CLI (compiled C++)
                    ├── OpenFOAM 9 (momentum solver)
                    ├── GDAL 3.4 / PROJ 8.2
                    └── Python 3.10 venv
```

### Bind mounts (host → container)

| Host path | Container path | Mode | Purpose |
|-----------|---------------|------|---------|
| `config/` | `/opt/mountain_windninja/config` | ro | Template, domains, env |
| `scripts/` | `/opt/mountain_windninja/scripts` | ro | Python code (no rebuild needed for changes) |
| `docker/` | `/opt/mountain_windninja/docker` | ro | Scheduler/cron scripts |
| `runtime/` | `/opt/mountain_windninja/runtime` | rw | Output, archives, logs |
| `static_data/` | `/opt/mountain_windninja/static_data` | rw | Terrain files + mesh cache |

**Key implication:** Changes to `scripts/`, `config/`, `docker/` take effect on next `mwn.sh run` without rebuilding the Docker image. Only dependency changes (pip packages, system libs, WindNinja recompile) require `mwn.sh build`.

### Environment variables

- `config/runtime.env` is injected via `env_file:` in `compose.yaml`
- `MWN_*` vars are read by `scripts/config_loader.py`
- `NINJAFOAM_MESH_COUNT` is read directly by WindNinja C++ (not by Python)
- `CUSTOM_SRTM_API_KEY` is read by WindNinja's `fetch_dem` tool

## Critical Gotchas (learned the hard way)

### 1. NINJAFOAM mesh cache corruption

WindNinja caches OpenFOAM meshes in `static_data/NINJAFOAM_<domain>_*/`. If a run fails mid-mesh (e.g., `moveDynamicMesh` error), the cache is corrupted and **every subsequent run fails** with `Can't open log.ninja`. The cache is NOT in `runtime/temp/` -- it's next to the elevation file.

**Fix:** `sudo rm -rf static_data/NINJAFOAM_*` or `mwn.sh clean`.

`mwn.sh run` now auto-cleans on failure to prevent this.

### 2. num_threads and domain size

`num_threads` in `template.cfg` controls OpenFOAM's parallel domain decomposition. Too many threads for a small domain causes `moveDynamicMesh` to fail. Rule of thumb:

- < 20 km domain: `num_threads = 4`
- 20-50 km domain: `num_threads = 6-8`
- 50+ km domain: `num_threads = 8+`

Currently set to 4 (safe default).

### 3. Docker image vs bind mounts

Scripts baked into the Docker image become stale when the repo is updated. The bind mounts for `scripts/`, `config/`, `docker/` were added specifically to avoid this. If you add new directories that contain runtime code, mount them in `compose.yaml` too.

### 4. OpenFOAM environment sourcing

OpenFOAM's `bashrc` uses unbound variables that crash under `set -eu`. Always source it with:
```bash
source /opt/openfoam9/etc/bashrc 2>/dev/null || true
```

### 5. fetch_dem sources

WindNinja's built-in `fetch_dem` only supports `srtm` (OpenTopography, 30m, needs API key), `gmted` (~250m), and `lcp` (LANDFIRE). The `us` source in `mwn.sh` is our custom addition using `gdalwarp` + USGS 3DEP VRT on S3, giving true 10m resolution without an API key.

### 6. LCP files need .prj sidecar

WindNinja requires a `.prj` projection file alongside LCP files. `mwn.sh fetch-lcp` auto-generates it via `gdalsrsinfo`. If manually placing LCP files, generate with:
```bash
gdalsrsinfo -o wkt my_file.lcp > my_file.prj
```

### 7. Public HRRR pastcast requires the patched image

Upstream WindNinja 3.12.2 hard-checks for GCS credentials before attempting to read public HRRR pastcast data. This repo patches upstream `src/ninja/gcp_wx_init.cpp` during the Docker build so `GS_NO_SIGN_REQUEST=YES` works against the public archive.

**Implication:** if reanalysis still fails with `Missing required GCS credentials`, the running image is stale. Pull latest code and rebuild with `./deploy/gcp/mwn.sh build`.

### 8. Synoptic validation has an external auth dependency

`mwn.sh synoptic-points` and `mwn.sh validate` require a Synoptic token with actual weather-data access. A 32-character token string in `config/runtime.env` is not enough if the Synoptic account itself is unauthorized.

### 9. momentum_flag conflicts with input_points_file

Upstream WindNinja rejects `input_points_file` when `momentum_flag = true` with:

```bash
Conflicting options 'momentum_flag' and 'input_points_file'
```

**Implication:** for momentum validation runs, do **not** use `--points-file`. Run the reanalysis normally and validate afterward with `mwn.sh validate-rasters`, which samples the output rasters at station coordinates.

### 10. Interrupted reanalysis runs do not resume

If a spot/preemptible instance dies mid-run, assume the active chunk is lost and rerun that chunk cleanly. There is no built-in checkpoint/resume behavior for partially completed reanalysis windows.

**Implication:** run historical validation studies in 24h or 72h chunks, not as one monolithic seasonal job.

### 11. NBM historical validation uses archived grid forcing

WindNinja does not expose an NBM `PASTCAST-*` model like HRRR. For
`validate-study --model NBM`, this repo fetches only the public NBM archive
`WIND` and `WDIR` 10 m GRIB records by HTTP byte range, converts them to
`run-grid` speed/direction inputs, runs one WindNinja timestep per valid hour,
and copies parent/WindNinja ASCII rasters into the same chunk validation layout
used by HRRR.

NBM has no `f000`; `--lead-hours` defaults to `1`. Use `--lead-hours 1` for the
closest past-comparison workflow and larger leads for forecast-skill validation.

### 12. Gridded forcing needs a DEM, not LCP

`run-grid`, `forcing-from-grib`, and NBM archive forcing use DEM-backed
gridded initialization. If a domain points at `name.lcp` and a sibling
`name.tif` exists in `static_data/`, the scripts automatically use the `.tif`
for gridded forcing while keeping the same domain key.

### 13. Berthoud validation is intentionally small and manifest-driven

The current Berthoud validation setup is a 10 km square centered on Berthoud Pass
and validates the explicit stations in
`config/stations/berthoud_pass_validation_manifest.csv`. The manifest currently
includes K0CO Berthoud Pass / Mines Peak AWOS and CABTP Berthoud Pass CAIC. Keep
`height_m_override` blank for CABTP until the actual anemometer height is known;
the workflow uses Synoptic wind sensor metadata when available, then the 10 m
study default.

Current measured scale:

- LCP: 337 x 336 cells at 30 m, about 10.1 x 10.1 km
- WindNinja output grid: 100 m cells from `config/template_validation.cfg`
- HRRR parent grid: about 3 km cells
- OpenFOAM mesh cache: `static_data/NINJAFOAM_berthoud_pass_158_4`
- Mesh: about 6,250 cells / 7,436 points
- Mesh cache size: about 29 MB
- Sampling map: `docs/assets/berthoud_validation_points.png`

For K0CO, raster validation samples the nearest WindNinja 100 m output cell
about 44 m from the station and the nearest parent-HRRR cell about 1.58 km from
the station.

The 2026-01-01 00:00 UTC through 2026-01-04 00:00 UTC pilot produced 73 matched
K0CO station-hours. WindNinja was biased low on speed but improved every
headline error metric versus parent HRRR:

- Speed MAE: 7.20 mph WindNinja vs 8.85 mph HRRR
- Direction MAE: 11.1 deg WindNinja vs 18.4 deg HRRR
- Vector RMSE: 10.26 mph WindNinja vs 12.53 mph HRRR

Use 24h chunks for month/year studies. `validate-study` reuses completed chunk
run directories and chunk summaries, so extending a window is safe:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --end 202602010000 \
  --chunk-hours 24
```

### 14. Validation template avoids visual artifacts

`berthoud_pass` uses `config/template_validation.cfg`, not `config/template.cfg`.
It keeps the same HRRR/momentum/diurnal/100 m physics path but disables KMZ and
shapefile output, while keeping WindNinja ASCII and parent-HRRR ASCII rasters
needed by `validate-rasters`.

## DEM Data Sources

| Source | Resolution | Coverage | API Key | mwn.sh arg |
|--------|-----------|----------|---------|------------|
| USGS 3DEP (via S3 VRT) | 10m | US | No | `us` |
| SRTM (via OpenTopography) | 30m | Global (60N-56S) | Yes (free) | `srtm` |
| GMTED2010 | ~250m | Global | No | `gmted` |
| LANDFIRE LCP | 30m + veg | US | No | `fetch-lcp` |

## Template Placeholders

`config/template.cfg` uses Python `str.format()` placeholders. If you add `{new_var}`, you must also pass `new_var=value` in `generate_config()` in `scripts/daily_run.py`, or the run will crash with `KeyError`.

Current placeholders: `{elevation_file}`, `{start_year}`, `{start_month}`, `{start_day}`, `{start_hour}`, `{start_minute}`, `{stop_year}`, `{stop_month}`, `{stop_day}`, `{stop_hour}`, `{stop_minute}`, `{forecast_duration}`, `{output_wind_height}`.

## Docker Build

- Base: Ubuntu 22.04
- OpenFOAM 9 (package from dl.openfoam.org)
- WindNinja compiled from source with `-D NINJA_QTGUI=OFF` (no GUI deps)
- Build-time upstream patch script: `docker/patch_windninja_public_pastcast.py`
- OpenFOAM custom libs (`libWindNinja.so`, `applyInit`) platform path: `linux64GccDPInt32Opt`
- Full build ~30 min; cached layers make rebuilds fast

## File Roles

| File | Role |
|------|------|
| `deploy/gcp/mwn.sh` | User-facing CLI. All user commands go through here. |
| `scripts/daily_run.py` | Core orchestrator: generates config, runs WindNinja, archives output |
| `scripts/config_loader.py` | Reads `runtime.env` + `domains.json`, provides typed config objects |
| `scripts/preflight_check.py` | Pre-run validation (files exist, CRS ok, WindNinja works) |
| `scripts/create_time_series.py` | Bundles hourly KMZ files into playable time-series KMZ |
| `scripts/raster_validation.py` | Samples nearest WindNinja and parent-HRRR rasters at station coordinates and compares against Synoptic |
| `scripts/synoptic_validation.py` | Builds station point CSVs and computes validation metrics |
| `scripts/validation_study.py` | Chunked Synoptic/model/WindNinja validation workflow using explicit station manifests |
| `scripts/nbm_archive.py` | Fetches archived NBM 10 m wind records by byte range and builds run-grid forcing |
| `scripts/forcing_from_grib.py` | Converts U/V or speed/direction GRIB/NetCDF fields into WindNinja grids |
| `scripts/validation_plots.py` | Pure-stdlib SVG/HTML plotting for completed validation sample chunks |
| `config/template.cfg` | WindNinja config template with placeholders |
| `config/template_validation.cfg` | Lean validation template, ASCII outputs only |
| `config/domains.json` | Maps domain names to terrain files and templates |
| `compose.yaml` | Docker Compose config (services, volumes, env_file) |
| `Dockerfile` | Builds the WindNinja + OpenFOAM environment |
| `docker/patch_windninja_public_pastcast.py` | Patches upstream WindNinja pastcast auth gate at build time |

## Testing

```bash
# Quick validation (no simulation)
./deploy/gcp/mwn.sh check

# Fast test run (1 hour forecast)
./deploy/gcp/mwn.sh run --hours 1

# Test with different wind height
./deploy/gcp/mwn.sh run --hours 1 --height 2

# Test different domain
./deploy/gcp/mwn.sh run --hours 1 --domain loveland_pass

# If anything breaks
./deploy/gcp/mwn.sh clean
```

## Future Work

- Higher mesh resolution testing (increase `NINJAFOAM_MESH_COUNT` gradually, test stability)
- Multi-domain batch runs
- GCS upload integration for sharing results
- Scheduled cron forecasts (scheduler service exists but needs production testing)
- Performance profiling on larger domains (30+ km)
- NDFD archive forcing and validation, following the NBM gridded-forcing pattern

## Handoff

Before handing this repo to another agent/operator:

- Confirm whether the active issue is image-level (`Dockerfile`, `docker/`) or bind-mounted runtime code (`scripts/`, `config/`).
- If the work touched `Dockerfile` or `docker/`, tell the next operator they must rebuild with `./deploy/gcp/mwn.sh build`.
- If the task involves historical validation, confirm whether Synoptic authorization is available before debugging the validation code path.
- Do not assume `config/template.cfg` reflects a canonical thread count; operators often tune it locally per VM/domain.

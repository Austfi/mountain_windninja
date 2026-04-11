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
| `config/template.cfg` | WindNinja config template with placeholders |
| `config/domains.json` | Maps domain names to terrain files and templates |
| `compose.yaml` | Docker Compose config (services, volumes, env_file) |
| `Dockerfile` | Builds the WindNinja + OpenFOAM environment |

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
- Support for additional weather models beyond HRRR

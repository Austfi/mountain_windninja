# AGENTS.md -- Mountain WindNinja

Context for AI agents working on this repo.

## What This Is

A Docker-based CLI wrapper around [WindNinja](https://github.com/firelab/windninja) for running wind simulations on cloud VMs (primarily GCP). The user interacts via `./deploy/gcp/mwn.sh <command>`.

There is also a separate research offshoot under `ml/residual_unet/` for learning
a machine-learning correction from WindNinja mass-solver output to momentum-solver
output. Keep that path adjacent to, but separate from, the operational
WindNinja/HRRR workflow.

## Architecture

```
Host (GCP VM or any Linux box)
  └── mwn.sh ← user-facing CLI, runs on host
        └── docker compose run --rm shell ← spins up container per command
              └── ghcr.io/austfi/mountain-windninja:3.12.2-herbie.3 or local image
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

The `ml/` tree is not part of the Docker runtime mount set. It is used locally
and in Colab for dataset building, model training, and evaluation.

### Environment variables

- `config/runtime.env` is injected via `env_file:` in `compose.yaml`
- `MWN_*` vars are read by `scripts/config_loader.py`
- `MWN_NUM_THREADS` overrides template `num_threads` for generated WindNinja configs
- `NINJAFOAM_MESH_COUNT` is read directly by WindNinja C++ (not by Python)
- `CUSTOM_SRTM_API_KEY` is read by WindNinja's `fetch_dem` tool

## Critical Gotchas (learned the hard way)

### 1. NINJAFOAM mesh cache corruption

WindNinja caches OpenFOAM meshes in `static_data/NINJAFOAM_<domain>_*/`. If a run fails mid-mesh (e.g., `moveDynamicMesh` error), the cache is corrupted and **every subsequent run fails** with `Can't open log.ninja`. The cache is NOT in `runtime/temp/` -- it's next to the elevation file.

**Fix, only when no WindNinja/OpenFOAM job is active:** `sudo rm -rf static_data/NINJAFOAM_*` or `mwn.sh clean`.

`mwn.sh run` now auto-cleans on failure to prevent this.

Never delete `static_data/NINJAFOAM_*` while a Docker/WindNinja validation job is
running. Check `docker ps` and `pgrep -af 'WindNinja_cli|daily_run.py|validate-study|gridded_run.py|mwn.sh|ml.residual_unet.hrrr_pair_runs'`
first. If a long run was interrupted, stop the host validation process as well
as the active Docker container before touching mesh caches.

### 2. num_threads and domain size

`num_threads` in `template.cfg` controls OpenFOAM's parallel domain decomposition. Too many threads for a small domain causes `moveDynamicMesh` to fail. Rule of thumb:

- < 20 km domain: `num_threads = 4`
- 20-50 km domain: `num_threads = 6-8`
- 50+ km domain: `num_threads = 8+`

Currently set to 4 in templates (safe default). On the current local machine,
`sysctl` reports 6 physical / 12 logical CPUs; use `MWN_NUM_THREADS=6` for a
high-thread trial, not 12. OpenFOAM momentum solving does not benefit from
hyperthreading.

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

### 11. Gridded forcing needs a DEM, not LCP

`run-grid` and `forcing-from-grib` use DEM-backed gridded initialization. If a
domain points at `name.lcp` and a sibling `name.tif` exists in `static_data/`,
the scripts automatically use the `.tif` for gridded forcing while keeping the
same domain key.

### 12. Berthoud validation is intentionally small and manifest-driven

The current Berthoud validation setup is a 10 km square centered on Berthoud Pass
and validates the explicit stations in
`config/stations/berthoud_pass_validation_manifest.csv`. The manifest currently
includes K0CO Berthoud Pass / Mines Peak AWOS, CABTP Berthoud Pass CAIC, and
USGS-394759105464101 Berthoud Pass USGS Meteorological Station. Keep
`height_m_override` blank for CABTP and USGS until actual anemometer heights are
known; the workflow uses provider wind sensor metadata when available, then the
10 m study default.

Current measured scale:

- LCP: 337 x 336 cells at 30 m, about 10.1 x 10.1 km
- WindNinja output grid: 100 m cells from `config/template_validation.cfg`
- HRRR parent grid: about 3 km cells
- OpenFOAM mesh cache: `static_data/NINJAFOAM_berthoud_pass_*` (suffix varies
  by mesh/run settings; inspect the current directory before acting)
- Mesh: about 6,250 cells / 7,436 points
- Mesh cache size: about 29 MB
- Sampling map: `docs/assets/berthoud_validation_points.png`

For K0CO, raster validation samples the nearest WindNinja 100 m output cell
about 44 m from the station and the nearest parent-HRRR cell about 1.58 km from
the station.

The 2026-01-01 00:00 UTC through 2026-02-01 00:00 UTC multistation HRRR
snapshot produced 2,145 deduplicated matched station-hours across K0CO, CABTP,
and USGS-394759105464101 in `plots/plot_summary.json`. Treat this as an
operational baseline, not a final research result, until CABTP and USGS
anemometer heights are verified.

Pooled HRRR comparison from that snapshot:

- Speed MAE: 7.88 mph WindNinja vs 10.10 mph HRRR
- Speed bias: 1.41 mph WindNinja vs 3.98 mph HRRR
- Direction MAE: 56.20 deg WindNinja vs 51.36 deg HRRR
- Vector RMSE: 13.67 mph WindNinja vs 14.56 mph HRRR

Station-level results are more important than pooled metrics. CABTP shows speed
MAE improvement but worse vector RMSE due direction error; K0CO shows modest
speed/vector improvement; the USGS low-wind site shows large speed/vector
improvement but direction metrics are mostly light-wind noise.

Use 24h chunks for month/year studies. `validate-study` reuses completed chunk
run directories and chunk summaries, so extending a window is safe:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --end 202602010000 \
  --chunk-hours 24
```

Plotting writes station-level metrics and terrain-backed sampling maps:
`plots/station_metrics.csv`, `plots/plot_summary.json`, and
`plots/sampling_map_<station>.png`. Do not reintroduce the old plain
`station_locations.svg`; it is redundant with the sampling maps.

NBM remains supported for native forecast runs through `mwn.sh run --model NBM`.
Do not add NBM historical validation unless WindNinja exposes a native
`PASTCAST-*` NBM model or the user explicitly chooses a separate archive-forcing
path.

### 13. Validation template avoids visual artifacts

`berthoud_pass` uses `config/template_validation.cfg`, not `config/template.cfg`.
It keeps the same HRRR/momentum/diurnal/100 m physics path but disables KMZ and
shapefile output, while keeping WindNinja ASCII and parent-HRRR ASCII rasters
needed by `validate-rasters`.

### 14. Artifact cleanup boundary

Before handoff, it is safe to delete disposable generated artifacts under
`runtime/temp/`, stale `static_data/PASTCAST-*` weather cache directories,
`.pytest_cache/`, `.ruff_cache/`, repo-level `__pycache__/`, and `.DS_Store`.
Only delete `static_data/NINJAFOAM_*` after confirming no WindNinja/OpenFOAM
container or host validation process is active.

Treat `runtime/validation/` as result storage, not generic temp. Delete old
validation roots only when the user explicitly agrees they are obsolete or the
final CSV/JSON/HTML outputs have been preserved elsewhere.

Do not delete `config/runtime.env`, `.venv/`, or local terrain inputs such as
`static_data/*.tif`, `static_data/*.lcp`, and `static_data/*.prj` unless they
have been backed up or are intentionally being regenerated.

### 15. K0CO height-adjusted HRRR research path

The K0CO height-adjusted HRRR workflow is exposed through:

```bash
./deploy/gcp/mwn.sh validate-k0co-height-hrrr
```

The current preferred K0CO experiment setting is:

```bash
--adjustment-setting exposure-gate-400m-10-80-cap
```

This setting:

- builds a GMTED2010 500 m terrain grid for the HRRR forcing footprint
- computes the 10 m to 80 m HRRR U/V blend at that coarse grid, not at the final
  WindNinja DEM resolution
- multiplies the terrain-height blend by a coarse 3 km TPI exposure gate
- caps adjusted speed relative to both HRRR 10 m and 80 m levels
- feeds the adjusted speed/direction grids into WindNinja gridded initialization

Full K0CO Jan 1-Apr 1 2026 results from
`runtime/validation/berthoud_pass_k0co_height_hrrr_exposure_gate_400m_10_80_cap/`:

| Result | Speed MAE | Bias | Direction MAE | Vector RMSE |
|--------|-----------|------|---------------|-------------|
| HRRR | 8.66 mph | -8.16 mph | 18.61 deg | 12.23 mph |
| Adjusted HRRR | 4.66 mph | -0.56 mph | 18.76 deg | 9.95 mph |
| WindNinja from HRRR | 8.89 mph | -7.80 mph | 17.77 deg | 12.76 mph |
| Momentum WindNinja from adjusted HRRR | 6.61 mph | -3.09 mph | 15.78 deg | 10.66 mph |
| Mass WindNinja from adjusted HRRR | 15.29 mph | +14.82 mph | 19.30 deg | 20.37 mph |

Interpretation: adjusted HRRR improves the forcing strongly at K0CO; momentum
WindNinja from adjusted HRRR improves over native WindNinja from HRRR; the mass
solver badly overspeeds K0CO from the adjusted forcing and should not be the
recommended K0CO path.

CABTP is nearby but behaves differently. Existing raster sampling of the
adjusted run at CABTP showed the exposure-gated adjusted HRRR barely changes
the raw HRRR because the coarse TPI exposure signal is weak there. Do not claim
the K0CO setting generalizes to CABTP without a clean completed two-station
native baseline.

Two-station K0CO+CABTP native baseline configs currently exist:

- `config/stations/berthoud_pass_k0co_cabtp_validation_manifest.csv`
- `config/studies/berthoud_pass_k0co_cabtp.json`
- `config/studies/berthoud_pass_k0co_cabtp_mass.json`

The native two-station mass run was intentionally paused after only two daily
chunks. Treat `runtime/validation/berthoud_pass_k0co_cabtp_mass/` as partial,
not a completed study.

### 16. Residual U-Net ML offshoot boundary

The residual U-Net code is under `ml/residual_unet/`. It should not change normal
`mwn.sh` behavior. The operational path remains WindNinja/HRRR first; ML uses
completed paired outputs as training data.

This is a momentum-solver emulator, not observation calibration. The current
practical direction is terrain-specific emulation for fixed 9.6 km boxes,
especially Breck/Tenmile and Keystone.

Current six-channel LCP-canopy inputs:

```text
z_rel, dzdx, dzdy, canopy_cover, u_mass, v_mass
```

`canopy_cover` is read directly from LANDFIRE LCP band 5
(`LF2024_CC_CONUS`) and is the only LCP-derived feature currently wired into the
ML pipeline. Do not combine 5-channel sources with 6-channel LCP-canopy sources;
`build_combined_dataset` now validates that all source channel lists match.

Current target:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

Preferred site-specific Colab entrypoint:

```text
ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb
```

Current GCS handoff artifacts:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/residual_unet_code.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/06_train_site_specific_9p6_colab.ipynb
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v1_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v1_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v2_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v2_dataset.zip
```

Latest site-specific held-out results:

```text
Breck/Tenmile V2 HRRR:
  mass vector RMSE: 3.697 m/s
  ML vector RMSE:   0.577 m/s
  improvement:      84.4%
  ML better pixels: 95.6%

Breck/Tenmile V2 controlled 15-degree:
  mass vector RMSE: 12.409 m/s
  ML vector RMSE:    2.881 m/s
  improvement:       76.8%

Keystone V2 HRRR:
  mass vector RMSE: 2.815 m/s
  ML vector RMSE:   0.421 m/s
  improvement:      85.0%
  ML better pixels: 96.5%

Keystone V2 controlled 15-degree:
  mass vector RMSE: 10.871 m/s
  ML vector RMSE:    3.543 m/s
  improvement:       67.4%
```

The package used 362 good full-year HRRR days and skipped three repeatedly
failing HRRR dates: `2025-06-27`, `2025-11-20`, and `2025-12-14`. The
V2 package independently evaluates held-out 7.5-degree midpoint controlled
directions. The next Colab ablation keeps the V2 datasets and writes separate
gradloss result folders:

```text
breck_tenmile_9p6_specific_lcp_canopy_v2_gradloss
keystone_9p6_specific_lcp_canopy_v2_gradloss
```

Read `docs/ml_residual_unet.md` for current ML status, result interpretation,
Colab steps, and cleanup boundaries. For returned Colab artifacts, read:

```text
MyDrive/windninja_ml/results/<run_name>/scorecard/scorecard_report.md
MyDrive/windninja_ml/results/_comparison/comparison_report.md
gs://mwn-ml-general-9p6-spring-nova-475120-r0/colab_results/
```

For “how many vectors in the 96 x 96 crop are close vs off,” use:

```text
ml_better_pixel_fraction
mass_better_pixel_fraction
ml_better_by_1mps_pixel_fraction
ml_worse_by_1mps_pixel_fraction
ml_vector_error_le_1p0mps_fraction
ml_vector_error_le_2p0mps_fraction
ml_vector_error_le_3p0mps_fraction
ml_vector_error_le_5p0mps_fraction
```

These fields are written to both `metrics.json` and `sample_metrics.csv`.

For terrain-specific models, do not lead with leave-one-terrain-out results.
Use same-terrain held-out HRRR test samples as the primary operational metric,
controlled-only samples as direction/speed stress tests, and the scorecard
breakdowns for high wind, season/month, 45-degree direction sector,
canopy-cover bin, and lee/windward slope bin.

Scorecard outputs live under:

```text
MyDrive/windninja_ml/results/<run_name>/scorecard/
```

The first file to read is `scorecard_report.md`. If the HRRR-only test metric is
good but high-wind, lee-side, or specific direction-sector rows regress, treat
the model as not ready for operational use on that terrain until those regimes
get more paired data or a model change.

Next ML work should focus on stricter day/event-level splits, reserving some
7.5-degree midpoint controlled cases for validation/test, and practical
inference checks against fresh paired mass/momentum runs. Do not chase larger
models until the split/evaluation design is sound.

Terrain-specific package definitions are config-driven through:

```text
ml/residual_unet/configs/site_specific_9p6_lcp_canopy.json
```

Add future fixed terrain boxes there before using
`ml.residual_unet.build_domain_specific_lcp_canopy --domain <site-key>`.

If raw Breck/Keystone HRRR and controlled outputs are already synced to GCS,
prefer the package-only V2 wrapper before running any new solvers:

```text
ml/residual_unet/package_breck_keystone_specific_v2_gcp.sh
```

The next terrain-expansion plan remains:

```text
docs/ml_next_terrain_expansion_plan.md
```

It stages these additional domains:

```text
copper_mountain_9p6 / copper_mountain_9p6_mass
vail_central_9p6 / vail_central_9p6_mass
monarch_pass_9p6 / monarch_pass_9p6_mass
```

Use `python3 -m ml.residual_unet.stage_terrain_expansion` to write the
fetch-terrain script, smoke HRRR plans, two one-week-per-month HRRR plans, and
controlled matrix scripts for these boxes. On a 24-core GCP VM, prefer the
staged `run_monthly_hrrr_parallel.sh` and `run_controlled_parallel.sh` scripts
for this three-domain wave; they run one 4-thread worker per domain and write
logs under the staged `logs/` directory. For unattended GCP execution, use the
staged `run_fetch_smoke_monthly_controlled_sync_and_stop.sh` wrapper so outputs
sync to GCS and the VM shuts down after completion or failure. After those
outputs exist, use
`python3 -m ml.residual_unet.build_mountain_general_lcp_canopy --domain-set
base4_plus_expansion3 --out
ml/residual_unet/data/processed/mountain_general_9p6_lcp_canopy_v2` for the
seven-domain V2 processed dataset.

The Vail box is a representative central/back-bowls 9.6 km box, not full Vail
resort coverage.

Residual U-Net inference is available for completed mass-solver runs:

```bash
.venv/bin/python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/<run_name>/best.pt \
  --mass-run runtime/temp/<mass_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph
```

Add `--momentum-run runtime/temp/<momentum_run>` when a paired momentum run
exists and metrics are needed. The command writes the trained `96 x 96` center
crop, not a full-domain raster.

### 17. Herbie forecast source is opt-in and image-level

The default forecast path remains WindNinja's native `wxModelInitialization`
with `wx_model_type`. Herbie runs are explicit:

```bash
./deploy/gcp/mwn.sh run --weather-source herbie --model HRRR --hours 1 --keep-temp --no-upload
```

The Herbie path prepares a local parent-model NetCDF under
`runtime/weather/herbie/<model>/<cycle>/<domain>/windninja_generic_GFS.nc` and
passes it to WindNinja with `forecast_filename`; it does not use
`griddedInitialization` ASCII forcing. The `GFS` filename suffix is only a
WindNinja generic-reader compatibility workaround; it is not a source-model
change.

This is a Docker image dependency change (`herbie-data`, `xarray`, `cfgrib`,
`netCDF4`, ecCodes). If Herbie imports or GRIB decoding fail inside the
container, rebuild with `./deploy/gcp/mwn.sh build-local` or pull a newer GHCR
image after one has been published and tested. Do not make Herbie the default
until native-vs-Herbie HRRR smoke tests are acceptably close for a small domain.

Only expose Herbie templates that make sense for WindNinja parent weather:
forecast grids with tested field-subset access for 10 m U/V and 2 m
temperature. The VM-tested Herbie suite is `HRRR`, `GFS`, `RRFS`, and
`HRRRAK`; keep RAP/NAM/NBM on WindNinja native until their Herbie subset path is
fixed. Do not list wave-only, storm-specific, climate, reforecast,
single-variable archive, or full-file-only global templates as normal
`mwn.sh run --model` choices.

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
- Build-time upstream patch scripts:
  `docker/patch_windninja_public_pastcast.py` and
  `docker/patch_windninja_generic_warp.py`
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
| `scripts/forcing_from_grib.py` | Converts U/V or speed/direction GRIB/NetCDF fields into WindNinja grids |
| `scripts/herbie_wx_model.py` | Opt-in Herbie adapter that writes WindNinja-compatible local weather-model NetCDF files |
| `scripts/k0co_height_hrrr_validation.py` | Focused K0CO adjusted-HRRR forcing and WindNinja validation harness |
| `scripts/hrrr_exposure_gate_assessment.py` | HRRR-level exposure-gate tuning and multistation diagnostics |
| `scripts/validation_plots.py` | Pure-stdlib SVG/HTML plotting for completed validation sample chunks |
| `config/template.cfg` | WindNinja config template with placeholders |
| `config/template_validation.cfg` | Lean validation template, ASCII outputs only |
| `config/domains.json` | Maps domain names to terrain files and templates |
| `compose.yaml` | Docker Compose config (services, volumes, env_file) |
| `Dockerfile` | Builds the WindNinja + OpenFOAM environment |
| `docker/patch_windninja_public_pastcast.py` | Patches upstream WindNinja pastcast auth gate at build time |
| `docker/patch_windninja_generic_warp.py` | Patches upstream WindNinja generic NetCDF warping for GDAL band arrays |
| `ml/residual_unet/` | Isolated ML offshoot for mass-to-momentum residual U-Net experiments |
| `docs/ml_residual_unet.md` | Current ML workflow, results, and cleanup boundary |

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
- Native forecast validation for NBM/NDFD products once a simple station workflow is defined
- Residual U-Net wrapper that runs mass solver and inference as one command
- Residual U-Net HRRR-channel v2 (`u_HRRR`, `v_HRRR`) after more paired HRRR data

## Handoff

Before handing this repo to another agent/operator:

- Confirm whether the active issue is image-level (`Dockerfile`, `docker/`) or bind-mounted runtime code (`scripts/`, `config/`).
- If the work touched `Dockerfile` or `docker/`, tell the next operator they must rebuild with `./deploy/gcp/mwn.sh build`.
- If the task involves historical validation, confirm whether Synoptic authorization is available before debugging the validation code path.
- Do not assume `config/template.cfg` reflects a canonical thread count; operators often tune it locally per VM/domain.
- State which ignored artifacts were cleaned and which local terrain inputs were preserved.

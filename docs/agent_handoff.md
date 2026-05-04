# Agent Handoff

This document is the quickest orientation point for another agent or operator picking up work in this repo.

## Current State

- Forecast runs are host-invoked through `./deploy/gcp/mwn.sh run` and execute inside the Docker image.
- Beginner setup is `mwn.sh init`, `mwn.sh fetch-terrain --center LAT LON --size-km N --domain KEY`, `mwn.sh check`, `mwn.sh smoke`, then `mwn.sh run`.
- `fetch-terrain` downloads DEM first as fallback and LCP second as active terrain. It accepts center/size, KML/KMZ area files, or explicit bbox. `fetch-dem`, `fetch-lcp`, and `domain create` remain available as advanced/manual paths.
- Historical reanalysis supports fixed windows through `--start` and `--end`.
- Native WindNinja reanalysis currently only supports HRRR
  (`PASTCAST-GCP-HRRR-CONUS-3-KM`). NBM is available as a native forecast model
  through `mwn.sh run --model NBM`, not as a historical `validate-study` mode.
- Point validation workflow exists through:
  - `./deploy/gcp/mwn.sh synoptic-points`
  - `./deploy/gcp/mwn.sh validate`
- Raster validation workflow exists through:
  - `./deploy/gcp/mwn.sh validate-rasters`
- Chunked validation studies exist through:
  - `./deploy/gcp/mwn.sh validate-study berthoud_pass ...`
- Synoptic is observation truth only. HRRR remains the model input to WindNinja.
- Current recommended Berthoud validation path is:
  - `validate-study berthoud_pass --start YYYYMMDDHHMM --pilot-hours 3`
  - inspect `runtime/validation/berthoud_pass/summary.json`
  - scale to `--end ... --chunk-hours 24`
- Berthoud station selection is explicit in
  `config/stations/berthoud_pass_validation_manifest.csv`. Add/remove station
  rows there before running `validate-study`.
- Berthoud sampling geometry is documented in
  validation plot outputs: `plots/sampling_map_<station>.png`, showing station,
  nearest WindNinja output cell, and nearest parent-model cell.
- The current local machine has 6 physical / 12 logical CPUs. Use
  `MWN_NUM_THREADS=6` for the high-thread trial; do not use 12 for OpenFOAM
  momentum runs.

## Critical Operational Notes

### Rebuild Boundary

Changes under these paths take effect on the next run without a rebuild:

- `scripts/`
- `config/`
- `docs/`

Changes under these paths require `./deploy/gcp/mwn.sh build`:

- `Dockerfile`
- `docker/`
- dependency changes
- anything that changes compiled WindNinja / GDAL / OpenFOAM behavior

### Public HRRR Pastcast

Upstream WindNinja 3.12.2 hard-checks for GCS credentials before reading public HRRR archive data. This repo patches upstream `src/ninja/gcp_wx_init.cpp` at build time via:

- [docker/patch_windninja_public_pastcast.py](../docker/patch_windninja_public_pastcast.py)

If reanalysis fails with `Missing required GCS credentials`, the most likely cause is a stale image that was not rebuilt after pulling Docker changes.

### Synoptic Validation

Validation needs a Synoptic token with actual account access. A token-shaped value in `config/runtime.env` is not enough if the Synoptic account is unauthorized.

Expected operator dependencies:

- `MWN_SYNOPTIC_TOKEN` in `config/runtime.env`
- active Synoptic weather-data access

Additional field-learned notes:

- If Synoptic metadata lacks a usable wind sensor height for one or more stations, `synoptic-points --default-height 10` is a practical pilot-time fallback.
- Upstream WindNinja rejects `input_points_file` when `momentum_flag = true` with `Conflicting options 'momentum_flag' and 'input_points_file'`.
- For momentum runs, do not use `--points-file` for validation. Use `validate-rasters` after the run instead.
- Interrupted reanalysis runs do not resume from the last completed hour. Rerun the chunk cleanly.
- On spot/preemptible instances, prefer daily or 72-hour chunks over one monolithic seasonal run.
- Use `validate-study --plan` to inspect long-window chunk paths before spending VM time.

## Common Failure Modes

### `moveDynamicMesh` / `Can't open log.ninja`

- Usually thread count or mesh-cache corruption.
- Run `./deploy/gcp/mwn.sh clean`.
- Reduce `num_threads` in `config/template.cfg`.

### `Missing required GCS credentials`

- For reanalysis, pull latest code and rebuild the image.
- Do not assume a GCS key is the right long-term fix; the repo intends to use public HRRR archive access through the patched image.

### Synoptic `403 Unauthorized`

- Verify account access in Synoptic customer console before changing repo code.

### `Conflicting options 'momentum_flag' and 'input_points_file'`

- This is an upstream WindNinja limitation, not a Synoptic auth problem.
- Remove `--points-file` from the run.
- Use `./deploy/gcp/mwn.sh validate-rasters` on the completed run directory instead.

## Recommended Verification Sequence

1. `git rev-parse --short HEAD`
2. `./deploy/gcp/mwn.sh check`
3. `./deploy/gcp/mwn.sh clean`
4. Run a 1-hour forecast smoke test if the issue is generic runtime stability.
5. Run a short reanalysis smoke test if the issue is historical/archive-specific.
6. For Synoptic validation, run a short `validate-rasters` smoke test on a completed reanalysis directory before scaling up to 24h+.
7. Use `pytest -q` locally before pushing repo changes.

## Important Files

- [deploy/gcp/mwn.sh](../deploy/gcp/mwn.sh)
- [scripts/daily_run.py](../scripts/daily_run.py)
- [scripts/validation_study.py](../scripts/validation_study.py)
- [scripts/raster_validation.py](../scripts/raster_validation.py)
- [scripts/synoptic_validation.py](../scripts/synoptic_validation.py)
- [Dockerfile](../Dockerfile)
- [docker/patch_windninja_public_pastcast.py](../docker/patch_windninja_public_pastcast.py)
- [config/template.cfg](../config/template.cfg)
- [config/template_validation.cfg](../config/template_validation.cfg)
- [config/domains.json](../config/domains.json)
- [config/studies/berthoud_pass.json](../config/studies/berthoud_pass.json)
- [config/stations/berthoud_pass_validation_manifest.csv](../config/stations/berthoud_pass_validation_manifest.csv)
- [config/stations/loveland_pass_validation_manifest.csv](../config/stations/loveland_pass_validation_manifest.csv)
- [docs/assets/berthoud_validation_points.png](assets/berthoud_validation_points.png)

## Recent Validation Snapshot

Berthoud validation uses a 10 km terrain box and explicit station manifest rows
for K0CO, CABTP, and USGS-394759105464101. Start with:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass --start 202601010000 --pilot-hours 3
```

The 2026-01-01 00:00 UTC through 2026-02-01 00:00 UTC multistation HRRR
snapshot produced 2,145 deduplicated matched station-hours in the plotting
output. Pooled HRRR comparison: WindNinja speed MAE 7.88 mph vs HRRR
10.10 mph, WindNinja vector RMSE 13.67 mph vs HRRR 14.56 mph, and WindNinja
direction MAE 56.20 deg vs HRRR 51.36 deg.

Use station-level metrics before making claims. `plots/station_metrics.csv`
shows CABTP speed improved but vector RMSE worsened, K0CO modestly improved,
and the USGS low-wind site improved speed/vector metrics while direction
metrics are mostly light-wind noise.

Do not reintroduce NBM historical validation unless WindNinja exposes a native
`PASTCAST-*` NBM model or the user explicitly chooses a separate archive-forcing
path. Keep the default validation workflow HRRR-only for now.

## Handoff Checklist

- State whether the next step needs a rebuild or not.
- State whether Synoptic access is available or blocked externally.
- State whether `config/template.cfg` was intentionally tuned locally and not committed.
- Leave one reproducible smoke-test command for the next operator.

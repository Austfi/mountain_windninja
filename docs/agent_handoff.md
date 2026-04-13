# Agent Handoff

This document is the quickest orientation point for another agent or operator picking up work in this repo.

## Current State

- Forecast runs are host-invoked through `./deploy/gcp/mwn.sh run` and execute inside the Docker image.
- Historical reanalysis supports fixed windows through `--start` and `--end`.
- Reanalysis currently only supports HRRR (`PASTCAST-GCP-HRRR-CONUS-3-KM`).
- Point validation workflow exists through:
  - `./deploy/gcp/mwn.sh synoptic-points`
  - `./deploy/gcp/mwn.sh validate`
- WindNinja point output includes both downscaled vectors (`u,v`) and parent-model vectors (`wx_u,wx_v`), so HRRR-vs-WindNinja comparisons are aligned from the same samples.

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

## Recommended Verification Sequence

1. `git rev-parse --short HEAD`
2. `./deploy/gcp/mwn.sh check`
3. `./deploy/gcp/mwn.sh clean`
4. Run a 1-hour forecast smoke test if the issue is generic runtime stability.
5. Run a short reanalysis smoke test if the issue is historical/archive-specific.
6. Use `pytest -q` locally before pushing repo changes.

## Important Files

- [deploy/gcp/mwn.sh](../deploy/gcp/mwn.sh)
- [scripts/daily_run.py](../scripts/daily_run.py)
- [scripts/synoptic_validation.py](../scripts/synoptic_validation.py)
- [Dockerfile](../Dockerfile)
- [docker/patch_windninja_public_pastcast.py](../docker/patch_windninja_public_pastcast.py)
- [config/template.cfg](../config/template.cfg)
- [config/domains.json](../config/domains.json)
- [config/stations/loveland_pass_validation_manifest.csv](../config/stations/loveland_pass_validation_manifest.csv)

## Handoff Checklist

- State whether the next step needs a rebuild or not.
- State whether Synoptic access is available or blocked externally.
- State whether `config/template.cfg` was intentionally tuned locally and not committed.
- Leave one reproducible smoke-test command for the next operator.

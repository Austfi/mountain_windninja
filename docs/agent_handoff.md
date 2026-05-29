# Agent Handoff

This document is the quickest orientation point for another agent or operator picking up work in this repo.

## Current State

- Last local cleanup: 2026-05-10. `runtime/temp/`, stale public-HRRR weather
  caches, and repo-level Python/test caches were cleaned. Local terrain inputs
  were kept. A newly created `static_data/NINJAFOAM_berthoud_pass_*` mesh cache
  was preserved after the active validation process was stopped.
- Local ignored config currently points `MWN_DOMAIN_ID=berthoud_pass` so local
  checks use terrain that exists in `static_data/`. `config/runtime.env` remains
  untracked and may differ per operator.
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
- The focused K0CO height-adjusted HRRR experiment exists through:
  - `./deploy/gcp/mwn.sh validate-k0co-height-hrrr ...`
- Current preferred K0CO adjusted-HRRR setting is
  `--adjustment-setting exposure-gate-400m-10-80-cap`.
- The paused two-station native baseline configs are:
  - `config/stations/berthoud_pass_k0co_cabtp_validation_manifest.csv`
  - `config/studies/berthoud_pass_k0co_cabtp.json`
  - `config/studies/berthoud_pass_k0co_cabtp_mass.json`
  The mass run was stopped after two daily chunks. Treat it as partial.
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
- The residual U-Net ML offshoot lives under `ml/residual_unet/` and is separate
  from the operational `mwn.sh` path. The current practical ML direction is
  site-specific Breck/Tenmile and Keystone momentum emulation; Berthoud
  `berthoud_combined_v1` remains an older baseline.
- A residual U-Net HRRR-pair process may run outside `mwn.sh` and launch
  short-lived Docker reanalysis chunks. Always include
  `ml.residual_unet.hrrr_pair_runs` in active-process checks before cleanup.

## Critical Operational Notes

### Residual U-Net ML Offshoot

The ML path learns a correction from WindNinja mass-solver output to
momentum-solver output. It is a momentum-emulator experiment, not an
observation-calibrated truth model. The current practical direction is
terrain-specific emulation for fixed 9.6 km boxes, especially Breck/Tenmile and
Keystone.

Current six-channel site-specific inputs:

```text
z_rel, dzdx, dzdy, canopy_cover, u_mass, v_mass
```

The target remains:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

Important files:

- `docs/ml_residual_unet.md`
- `docs/ml_generalization_data_plan.md`
- `docs/ml_next_terrain_expansion_plan.md`
- `ml/residual_unet/README.md`
- `ml/residual_unet/hrrr_pair_runs.py`
- `ml/residual_unet/build_controlled_dataset.py`
- `ml/residual_unet/build_domain_specific_lcp_canopy.py`
- `ml/residual_unet/emulator_scorecard.py`
- `ml/residual_unet/compare_results.py`
- `ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb`

Current site-specific Colab/GCS artifacts:

```text
bucket: gs://mwn-ml-general-9p6-spring-nova-475120-r0
datasets:
  drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v1_dataset.zip
  drive_upload/keystone_9p6_specific_lcp_canopy_v1_dataset.zip
notebook:
  drive_upload/06_train_site_specific_9p6_colab.ipynb
results:
  colab_results/breck_tenmile_9p6_specific_lcp_canopy_v1/
  colab_results/keystone_9p6_specific_lcp_canopy_v1/
  colab_results/_comparison/
```

Latest held-out same-terrain results:

```text
Breck/Tenmile HRRR: mass RMSE 3.697 m/s, ML RMSE 0.626 m/s, 83.1% improvement, 95.6% better pixels
Breck/Tenmile controlled 15-degree: mass RMSE 12.409 m/s, ML RMSE 2.392 m/s, 80.7% improvement
Keystone HRRR: mass RMSE 2.815 m/s, ML RMSE 0.452 m/s, 83.9% improvement, 96.4% better pixels
Keystone controlled 15-degree: mass RMSE 10.871 m/s, ML RMSE 2.897 m/s, 73.4% improvement
```

The Breck/Keystone data package used 362 good full-year HRRR days and skipped
three repeatedly failing HRRR dates: 2025-06-27, 2025-11-20, and 2025-12-14.
The 7.5-degree midpoint controlled set was included as train-only in this
package, so it helps training but is not independently evaluated yet.

The VM `mwn-ml-general-9p6` was terminated after packaging/training handoff.
Refresh live GCP state before making operational decisions; this document is
handoff context, not a live monitor.

Generated ML data/checkpoints are ignored. Preserve `best.pt`, `metrics.json`,
`sample_metrics.csv`, `train_log.csv`, scorecards, and comparison reports unless
the user explicitly wants to discard returned Colab artifacts.

Next ML work should focus on stricter day/event-level splits, reserving some
7.5-degree midpoint controlled cases for validation/test, and practical
inference checks against fresh paired mass/momentum runs before changing model
architecture.

The prepared large HRRR-pair plan is local and ignored:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501/
```

It covers 2025-10-01 00Z through 2026-05-01 00Z in 24h chunks and skips existing
complete chunks. Do not run it while another WindNinja/OpenFOAM container is
active.

The prepared 4-vs-6 thread benchmark is also local and ignored:

```text
runtime/ml/residual_unet/thread_benchmark/run_momentum_thread_benchmark.sh
```

Run it only when no other WindNinja/OpenFOAM job is active.

### Artifact Cleanup Boundary

Do not run cleanup while a validation container is active. Check `docker ps` and
any active `tmux` or `screen` session first. Also check for the host-side
process, because `docker stop` alone can leave `validate-study` alive and able
to relaunch a new short-lived container:

```bash
pgrep -af 'WindNinja_cli|daily_run.py|validate-study|gridded_run.py|mwn.sh|ml.residual_unet.hrrr_pair_runs'
```

Safe handoff cleanup targets:

- `runtime/temp/*`
- stale run directories under `runtime/forecasts/`, `runtime/archives/`, or
  `runtime/logs/` when the user no longer needs them
- `runtime/ml/residual_unet/*` if local ML plans/raw data are no longer needed
- `static_data/PASTCAST-*` weather archive cache directories
- `static_data/NINJAFOAM_*` only after confirming no WindNinja/OpenFOAM job is active
- `ml/residual_unet/data/processed/`
- `ml/residual_unet/outputs/`
- `.pytest_cache/`, `.ruff_cache/`, and `__pycache__/`
- `.DS_Store`

Preserve unless explicitly backed up or intentionally regenerating:

- `config/runtime.env` (local secrets/settings)
- `.venv/` (local dev environment)
- `ml/residual_unet/colab/results/` returned Colab metrics/checkpoints unless
  intentionally archived or regenerated
- `runtime/validation/` final CSV/JSON/HTML outputs unless the user explicitly
  confirms the study root is obsolete
- terrain inputs under `static_data/*.tif`, `static_data/*.lcp`, and
  `static_data/*.prj`

### K0CO Adjusted-HRRR Result Snapshot

The completed K0CO exposure-gated adjusted-HRRR run is under:

```text
runtime/validation/berthoud_pass_k0co_height_hrrr_exposure_gate_400m_10_80_cap/
```

Full Jan 1-Apr 1 2026 K0CO metrics:

| Result | Speed MAE | Bias | Direction MAE | Vector RMSE |
|--------|-----------|------|---------------|-------------|
| HRRR | 8.66 mph | -8.16 mph | 18.61 deg | 12.23 mph |
| Adjusted HRRR | 4.66 mph | -0.56 mph | 18.76 deg | 9.95 mph |
| WindNinja from HRRR | 8.89 mph | -7.80 mph | 17.77 deg | 12.76 mph |
| Momentum WindNinja from adjusted HRRR | 6.61 mph | -3.09 mph | 15.78 deg | 10.66 mph |
| Mass WindNinja from adjusted HRRR | 15.29 mph | +14.82 mph | 19.30 deg | 20.37 mph |

Practical read: use momentum WindNinja for the adjusted-HRRR path at K0CO. The
mass-solver adjusted-HRRR result overspeeds badly.

CABTP side assessment from existing adjusted rasters:

| Result | Speed MAE | Bias | Direction MAE | Vector RMSE |
|--------|-----------|------|---------------|-------------|
| HRRR | 4.75 mph | +3.47 mph | 24.35 deg | 8.86 mph |
| Adjusted HRRR exposure gate | 4.71 mph | +3.42 mph | 24.41 deg | 8.82 mph |
| Momentum WN from adjusted HRRR | 6.53 mph | -5.68 mph | 51.96 deg | 12.20 mph |
| Mass WN from adjusted HRRR | 6.95 mph | +6.59 mph | 21.93 deg | 10.40 mph |

CABTP is not a K0CO-like validation win. Its coarse TPI exposure weight is low,
so the selected exposure gate mostly leaves HRRR unchanged there.

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
- [docker/patch_windninja_generic_warp.py](../docker/patch_windninja_generic_warp.py)

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
- [scripts/k0co_height_hrrr_validation.py](../scripts/k0co_height_hrrr_validation.py)
- [scripts/raster_validation.py](../scripts/raster_validation.py)
- [scripts/synoptic_validation.py](../scripts/synoptic_validation.py)
- [Dockerfile](../Dockerfile)
- [docker/patch_windninja_public_pastcast.py](../docker/patch_windninja_public_pastcast.py)
- [docker/patch_windninja_generic_warp.py](../docker/patch_windninja_generic_warp.py)
- [config/template.cfg](../config/template.cfg)
- [config/template_validation.cfg](../config/template_validation.cfg)
- [config/domains.json](../config/domains.json)
- [config/studies/berthoud_pass.json](../config/studies/berthoud_pass.json)
- [config/stations/berthoud_pass_validation_manifest.csv](../config/stations/berthoud_pass_validation_manifest.csv)
- [config/stations/loveland_pass_validation_manifest.csv](../config/stations/loveland_pass_validation_manifest.csv)
- [docs/assets/berthoud_validation_points.png](assets/berthoud_validation_points.png)
- [docs/ml_residual_unet.md](ml_residual_unet.md)
- [ml/residual_unet/README.md](../ml/residual_unet/README.md)

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
- State which ignored artifacts were cleaned and which terrain inputs were preserved.
- Leave one reproducible smoke-test command for the next operator.

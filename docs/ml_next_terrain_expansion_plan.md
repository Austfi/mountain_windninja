# ML Terrain Expansion Plan

This is the next data-expansion plan after the four-domain LCP-canopy model.
The goal is to add new terrain shapes before adding more input channels. The
current holdout results show useful signal on Loveland/A-Basin and
Breck/Tenmile, but Keystone HRRR exposed an overcorrection/generalization risk.
More terrain diversity is the cleanest next test.

## Proposed 9.6 km Boxes

| Domain | Mass domain | Center | Bbox N E S W | Notes |
|---|---|---:|---:|---|
| `copper_mountain_9p6` | `copper_mountain_9p6_mass` | `39.4840,-106.1516` | `39.52716738 -106.09566938 39.44083262 -106.20753062` | Covers Copper village/top station through the south ridge/Tucker side. |
| `vail_central_9p6` | `vail_central_9p6_mass` | `39.6060,-106.3740` | `39.64916738 -106.31797096 39.56283262 -106.43002904` | Representative Vail central/back-bowls terrain. One 9.6 km box does not cover the full Vail resort footprint. |
| `monarch_pass_9p6` | `monarch_pass_9p6_mass` | `38.5103,-106.3395` | `38.55346738 -106.28433376 38.46713262 -106.39466624` | Covers Monarch ski area / Monarch Pass divide terrain. |

Review KMLs:

```text
docs/assets/ml_training_boxes/copper_mountain_9p6_bbox.kml
docs/assets/ml_training_boxes/vail_central_9p6_bbox.kml
docs/assets/ml_training_boxes/monarch_pass_9p6_bbox.kml
```

## Stage The Wave

Use the staging helper first. It writes the terrain-fetch script, one-domain
HRRR smoke plans, two one-week-per-month HRRR plans, and controlled-matrix
scripts for the three new boxes.

```bash
python3 -m ml.residual_unet.stage_terrain_expansion
```

Default output:

```text
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/
runtime/ml/residual_unet/hrrr_pairs/copper_mountain_9p6_smoke/
runtime/ml/residual_unet/hrrr_pairs/copper_mountain_9p6_hrrr_lcp_canopy_v1/
runtime/ml/residual_unet/hrrr_pairs/vail_central_9p6_smoke/
runtime/ml/residual_unet/hrrr_pairs/vail_central_9p6_hrrr_lcp_canopy_v1/
runtime/ml/residual_unet/hrrr_pairs/monarch_pass_9p6_smoke/
runtime/ml/residual_unet/hrrr_pairs/monarch_pass_9p6_hrrr_lcp_canopy_v1/
runtime/ml/residual_unet/raw/controlled_9p6_15deg/<domain>/
```

The top-level scripts are:

```text
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/fetch_terrain.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_smoke_all.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_monthly_hrrr_all.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_monthly_hrrr_parallel.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_controlled_all.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_controlled_parallel.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_fetch_smoke_monthly_controlled_sync_and_stop.sh
```

For a subset while testing:

```bash
python3 -m ml.residual_unet.stage_terrain_expansion \
  --domain copper_mountain_9p6 \
  --label copper_only_setup
```

Coordinate references used for initial centers:

- Copper Mountain: village/resort coordinate near `39.501419,-106.1516265`;
  CDOT station evidence also places a high Copper Mountain Resort station near
  `39.467,-106.15`.
- Vail: public coordinate references place Vail Ski Resort near
  `39.6391,-106.3738`; this training box is shifted south to include more
  mountain terrain and bowls.
- Monarch: topo references place Monarch Ski Area near
  `38.5102745,-106.3394693`.

Reference URLs:

```text
https://www.distancesto.com/coordinates/us/copper-mountain-latitude-longitude/history/935.html
https://spl.cde.state.co.us/artemis/traserials/tra18p28internet/tra18p282015internet.pdf
https://latitude.to/satellite-map/us/united-states/8818/vail-ski-resort
https://www.topozone.com/colorado/chaffee-co/locale/monarch-ski-area/
```

## Terrain Fetch

Run these on the GCP VM or a machine with the normal Docker/GDAL terrain path.
The domains are already staged in `config/domains.json` with validation and mass
templates, so `fetch-terrain` should preserve those templates when it updates
the terrain path.

If you used the staging helper, run this instead of copying the three individual
commands below:

```bash
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/fetch_terrain.sh
```

```bash
./deploy/gcp/mwn.sh fetch-terrain \
  --center 39.4840 -106.1516 \
  --size-km 9.6 \
  --domain copper_mountain_9p6 \
  --label "Copper Mountain 9.6 km"

./deploy/gcp/mwn.sh fetch-terrain \
  --center 39.6060 -106.3740 \
  --size-km 9.6 \
  --domain vail_central_9p6 \
  --label "Vail Central/Back Bowls 9.6 km"

./deploy/gcp/mwn.sh fetch-terrain \
  --center 38.5103 -106.3395 \
  --size-km 9.6 \
  --domain monarch_pass_9p6 \
  --label "Monarch Pass 9.6 km"
```

Expected terrain files:

```text
static_data/copper_mountain_9p6.tif
static_data/copper_mountain_9p6.lcp
static_data/copper_mountain_9p6.prj
static_data/vail_central_9p6.tif
static_data/vail_central_9p6.lcp
static_data/vail_central_9p6.prj
static_data/monarch_pass_9p6.tif
static_data/monarch_pass_9p6.lcp
static_data/monarch_pass_9p6.prj
```

## Smoke Runs

Generate one 24-hour HRRR mass/momentum pair for each new domain before starting
any long batch.

If you used the staging helper:

```bash
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_smoke_all.sh
```

```bash
.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202601010000 \
  --end 202601020000 \
  --chunk-hours 24 \
  --threads 4 \
  --momentum-domain copper_mountain_9p6 \
  --mass-domain copper_mountain_9p6_mass \
  --label copper_mountain_9p6_smoke \
  --terrain-domain copper_mountain_9p6 \
  --write-run-script

.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202601010000 \
  --end 202601020000 \
  --chunk-hours 24 \
  --threads 4 \
  --momentum-domain vail_central_9p6 \
  --mass-domain vail_central_9p6_mass \
  --label vail_central_9p6_smoke \
  --terrain-domain vail_central_9p6 \
  --write-run-script

.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202601010000 \
  --end 202601020000 \
  --chunk-hours 24 \
  --threads 4 \
  --momentum-domain monarch_pass_9p6 \
  --mass-domain monarch_pass_9p6_mass \
  --label monarch_pass_9p6_smoke \
  --terrain-domain monarch_pass_9p6 \
  --write-run-script
```

Then run each generated `run_hrrr_pairs.sh` only after checking no other
WindNinja/OpenFOAM job is active.

## Controlled Matrix

After HRRR smoke passes, stage controlled 15-degree cases for each domain:

If you used the staging helper, configs are already written. Run:

```bash
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_controlled_all.sh
```

```bash
.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile training \
  --domain-label copper_mountain_9p6 \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/copper_mountain_9p6 \
  --write-configs \
  --write-run-script

.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile training \
  --domain-label vail_central_9p6 \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/vail_central_9p6 \
  --write-configs \
  --write-run-script

.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile training \
  --domain-label monarch_pass_9p6 \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/monarch_pass_9p6 \
  --write-configs \
  --write-run-script
```

## First Long Batch Recommendation

Do not run a full-year-every-day batch first. Use the same robust but bounded
pattern as the current four-domain LCP-canopy dataset:

- two 7-day HRRR weeks per month
- May 2025 through April 2026
- 15-degree controlled matrix
- LCP canopy channel
- 4 OpenFOAM threads per worker

On a `c4-standard-24`, run at most three domains in parallel. For seven total
domains, use waves:

```text
wave 1: copper_mountain_9p6, vail_central_9p6, monarch_pass_9p6
wave 2: optional reruns/failures or one extra Vail box if added
```

The staged parallel runners start one 4-thread worker per new domain and write
logs under `runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/logs/`:

```bash
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_monthly_hrrr_parallel.sh
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_controlled_parallel.sh
```

For the unattended billable GCP run, prefer the sync/stop wrapper. It fetches
terrain, runs smoke, runs the parallel HRRR batch, runs the parallel controlled
batch, syncs `static_data`, `runtime/temp`, and `runtime/ml/residual_unet` to
GCS, then shuts the VM down:

```bash
runtime/ml/residual_unet/terrain_expansion/terrain_expansion_wave1_v1/run_fetch_smoke_monthly_controlled_sync_and_stop.sh
```

The next combined processed dataset should be versioned separately, for example:

```text
ml/residual_unet/data/processed/mountain_general_9p6_lcp_canopy_v2
```

Build that V2 dataset after the new HRRR and controlled outputs complete:

```bash
python3 -m ml.residual_unet.build_mountain_general_lcp_canopy \
  --domain-set base4_plus_expansion3 \
  --out ml/residual_unet/data/processed/mountain_general_9p6_lcp_canopy_v2
```

Keep V2 separate from the current `mountain_general_9p6_lcp_canopy_v1` so model
progression can be compared cleanly in Colab.

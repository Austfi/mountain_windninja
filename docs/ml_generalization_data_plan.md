# Residual U-Net Generalization Data Plan

Status note: this is now a historical/secondary plan. The four-domain
generalization work produced useful signal, but the current practical ML
direction is terrain-specific Breck/Tenmile and Keystone momentum emulation.
Use [the residual U-Net guide](ml_residual_unet.md) for current results,
Colab artifacts, and next steps.

This plan skips Vail for now and builds the first general mountain dataset from
four 9.6 km boxes:

| Domain | Mass domain | Center | Bbox N E S W |
| --- | --- | --- | --- |
| `berthoud_pass` | `berthoud_pass_mass` | existing domain | existing domain |
| `breck_tenmile_9p6` | `breck_tenmile_9p6_mass` | `39.4685,-106.0785` | `39.51166738 -106.02258184 39.42533262 -106.13441816` |
| `keystone_9p6` | `keystone_9p6_mass` | `39.56683262,-105.9290` | `39.61000000 -105.87300262 39.52366524 -105.98499738` |
| `loveland_abasin_9p6` | `loveland_abasin_9p6_mass` | `39.6543,-105.8962` | `39.69746738 -105.84013183 39.61113262 -105.95226817` |

The next terrain-expansion plan after this four-domain baseline is:

```text
docs/ml_next_terrain_expansion_plan.md
```

It stages Copper Mountain, a representative central/back-bowls Vail box, and
Monarch Pass as the next 9.6 km training domains.

KML review files:

```text
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_smoke/breck_tenmile_9p6_bbox.kml
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_smoke/keystone_9p6_bbox.kml
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_smoke/loveland_abasin_9p6_bbox.kml
```

## Repo And Branch Organization

The original generalization work used a separate branch from the older K0CO
validation branch. Current ML documentation and site-specific notebooks are now
tracked on `main`; check `git branch --show-current` before assuming an old
branch name applies.

```text
ml-generalization-data-build
```

Commit source, configs, notebooks, tests, and small docs. Do not commit the
generated runtime/data products:

```text
runtime/
static_data/
ml/residual_unet/data/processed/
ml/residual_unet/outputs/
ml/residual_unet/colab/
```

Those paths are ignored intentionally. `runtime/` and `static_data/` are still
operationally important while the GCP run is active, so do not delete them
during repo cleanup.

## Current Starting Evidence

The Breck/Tenmile 24h V1 smoke used true WindNinja momentum as the target and
the existing `berthoud_combined_v1` checkpoint as the ML model:

```text
window: 2026-01-01 00Z through 2026-01-02 00Z
samples: 25 hourly rasters
mass vector RMSE: 4.291 m/s
V1 ML vector RMSE: 3.731 m/s
overall vector RMSE improvement: 13.0%
per-hour outcome: 20 improved, 5 worse
```

This is enough to justify a broader data run, but not enough to justify a huge
run before V2 and multi-domain tests.

## Historical Cloud Run Snapshot

Historical production data build snapshot from 2026-05-19 17:04 UTC:

```text
project: spring-nova-475120-r0
zone: us-central1-a
vm: mwn-ml-general-9p6
bucket: gs://mwn-ml-general-9p6-spring-nova-475120-r0
tmux session: mwn-monthly2
runner: runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly2_202505_202604_v1/run_monthly2_hrrr_sync_and_stop.sh
```

This second run is HRRR-only data generation. It adds one non-overlapping
7-day week per month over the same four domains:

```text
2025-05-15 00Z -> 2025-05-22 00Z
2025-06-22 00Z -> 2025-06-29 00Z
2025-07-01 00Z -> 2025-07-08 00Z
2025-08-08 00Z -> 2025-08-15 00Z
2025-09-15 00Z -> 2025-09-22 00Z
2025-10-22 00Z -> 2025-10-29 00Z
2025-11-01 00Z -> 2025-11-08 00Z
2025-12-08 00Z -> 2025-12-15 00Z
2026-01-15 00Z -> 2026-01-22 00Z
2026-02-22 00Z -> 2026-03-01 00Z
2026-03-01 00Z -> 2026-03-08 00Z
2026-04-08 00Z -> 2026-04-15 00Z
```

It runs three 4-thread domain workers first, then Loveland/A-Basin as the
fourth worker. The wrapper syncs `runtime/temp` and
`runtime/ml/residual_unet` to the bucket and shuts the VM down when finished.

The previous production data build was:

```text
tmux session: mwn-monthly
runner: runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

That first monthly run produced paired mass/momentum output for one 7-day HRRR
week per month over Berthoud, Breck/Tenmile, Keystone, and Loveland/A-Basin, and
controlled 15-degree speed/direction forcing over the same domains.

The HRRR pair runner quarantines incomplete run directories, cleans the failed
domain mesh cache, and retries once before counting a solver failure.

Refresh live status before making operational decisions; this document is a
handoff/runbook, not a live monitor.

## First Cloud Batch

The original first batch remains staged as a smaller 12-week HRRR run. The
larger robust batch below is the preferred training-data build if cost and
runtime are acceptable.

Use twelve 7-day HRRR windows per domain:

```text
2025-10-01 00Z -> 2025-10-08 00Z
2025-10-15 00Z -> 2025-10-22 00Z
2025-11-01 00Z -> 2025-11-08 00Z
2025-12-01 00Z -> 2025-12-08 00Z
2025-12-15 00Z -> 2025-12-22 00Z
2026-01-01 00Z -> 2026-01-08 00Z
2026-01-15 00Z -> 2026-01-22 00Z
2026-02-01 00Z -> 2026-02-08 00Z
2026-02-15 00Z -> 2026-02-22 00Z
2026-03-01 00Z -> 2026-03-08 00Z
2026-04-01 00Z -> 2026-04-08 00Z
2026-05-01 00Z -> 2026-05-08 00Z
```

Planned scale:

```text
4 domains x 84 domain-days x about 25 hourly rasters = about 8,400 new samples
existing berthoud_combined_v2 = 4,358 samples
first general dataset target = about 12,700 samples before controlled forcing
```

Staged per-domain plans:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_pass_general_9p6_12w_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_general_12w_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_general_12w_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_general_12w_v1/plan.json
```

On the current `c4-standard-24` VM, run three domain workers first and the
remaining domain afterward:

```bash
runtime/ml/residual_unet/hrrr_pairs/general_9p6_12w_v1/run_3plus1_domains_c4_24.sh
```

Only use the four-worker runner below on a larger VM such as `c4-standard-32`
or after a deliberate benchmark shows it is faster without oversubscribing
OpenFOAM:

```bash
runtime/ml/residual_unet/hrrr_pairs/general_9p6_12w_v1/run_all_domains_parallel.sh
```

Each domain worker uses 24h chunks and skips completed chunks when rerun, so the
batch can recover from VM shutdown or Spot preemption.

## Smoke Before Cloud Batch

The smoke tests are data-generation checks only: mass run, momentum run, keep
paired outputs. They intentionally skip ML inference because inference is not
needed to build the training dataset.

The local/cloud smoke scripts are:

```bash
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_smoke/run_hrrr_pairs.sh
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_smoke/run_hrrr_pairs.sh
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_smoke/run_hrrr_pairs.sh
```

Run these only after visually checking the KML coverage. If either new terrain
has a mesh or momentum failure, fix that box before starting any long batch.

## Cloud VM Recommendation

For the no-Vail batch, prefer this when quota allows it:

```text
region: us-central1
machine: c4-standard-32
disk: 500 GB hyperdisk-balanced
WindNinja threads per worker: 4
parallel workers: 4 domains
```

This is intentionally sized for physical-core behavior. Google documents that a
vCPU is one hardware thread and that a CPU uses two threads per core. Therefore
`c4-standard-32` should be treated as roughly 16 physical cores for OpenFOAM
sizing. Four domain workers at four OpenFOAM ranks each use 16 physical-core
equivalents.

Do not run the four-worker batch on `c4-standard-16` with `threads=4`; that
would ask for 16 OpenFOAM ranks on roughly 8 physical cores. It might run, but
it is oversubscribed and is not the efficient/stable plan. If using
`c4-standard-16`, run only two domain workers at a time or regenerate the plans
with `--threads 2`.

Current project quota fallback for the active monthly robust batch:

```text
machine: c4-standard-24
disk: 300 GB hyperdisk-balanced
parallel workers: 3 domains first, then 1 remaining domain
runner: runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

This uses 12 physical-core equivalents at a time and fits the current 24-vCPU
C4 family quota. It is slower than `c4-standard-32` but avoids waiting for a
quota increase.

Use the auto-sync/auto-stop wrapper for the billable long run:

```bash
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

That wrapper runs the `c4-standard-24` monthly HRRR 3+1 batch, then the
controlled 3+1 batch, syncs both `runtime/temp` and
`runtime/ml/residual_unet` to GCS, then shuts down the VM. The VM also has a
36-hour max-run-duration stop guard.

GCS output layout:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/runtime_temp/
gs://mwn-ml-general-9p6-spring-nova-475120-r0/runtime_ml/
gs://mwn-ml-general-9p6-spring-nova-475120-r0/helpers/download_outputs_from_gcs.sh
```

To pull the completed outputs back into this checkout:

```bash
gcloud storage rsync -r gs://mwn-ml-general-9p6-spring-nova-475120-r0/runtime_temp runtime/temp
gcloud storage rsync -r gs://mwn-ml-general-9p6-spring-nova-475120-r0/runtime_ml runtime/ml/residual_unet
```

## Robust Full-Year Plus Controlled Batch

The robust batch expands the natural-weather HRRR data to a full year and adds
controlled speed/direction forcing over every 9.6 km terrain box.

Full-year HRRR window:

```text
2025-05-01 00Z -> 2026-05-01 00Z
```

Staged HRRR plans:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_pass_hrrr_20250501_20260501_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_hrrr_20250501_20260501_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_hrrr_20250501_20260501_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_hrrr_20250501_20260501_v1/plan.json
```

HRRR scale:

```text
4 domains x 365 domain-days x about 25 hourly rasters = about 36,500 natural-weather samples
4 domains x 365 chunks x 2 solvers = 2,920 WindNinja solver runs
```

Controlled forcing matrix:

```text
domains: berthoud_pass, breck_tenmile_9p6, keystone_9p6, loveland_abasin_9p6
speeds: 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80 mph
directions: 0, 15, 30, ..., 345 deg
cases: 11 speeds x 24 directions x 4 domains = 1,056 cases
solver runs: 2,112 mass/momentum controlled runs
```

Controlled outputs are staged under:

```text
runtime/ml/residual_unet/raw/controlled_9p6_15deg/
```

Use this wrapper for the full robust billable run:

```bash
runtime/ml/residual_unet/hrrr_pairs/general_9p6_20250501_20260501_v1/run_full_year_hrrr_plus_controlled_sync_and_stop.sh
```

That wrapper runs the full-year HRRR 3+1 batch first. If HRRR succeeds, it runs
the 15-degree controlled 3+1 batch. It then syncs `runtime/temp` and
`runtime/ml/residual_unet` to GCS and shuts down the VM.

This full robust batch is materially larger than the 12-week batch. On the
current `c4-standard-24` VM, expect this to exceed the previous `$50` alert
unless interrupted/resumed in stages or moved to Spot. Increase the VM
max-run-duration before launch if you intend to let it finish in one pass; the
existing 36-hour guard is likely too short for the full-year-plus-controlled
batch.

## Robust Monthly HRRR Plus Controlled Batch

The preferred cost-balanced robust batch keeps year-round HRRR coverage but
samples one 7-day HRRR window per month, then runs the same 15-degree controlled
matrix over all four terrain boxes.

Monthly HRRR windows:

```text
2025-05-01 00Z -> 2025-05-08 00Z
2025-06-08 00Z -> 2025-06-15 00Z
2025-07-15 00Z -> 2025-07-22 00Z
2025-08-22 00Z -> 2025-08-29 00Z
2025-09-01 00Z -> 2025-09-08 00Z
2025-10-08 00Z -> 2025-10-15 00Z
2025-11-15 00Z -> 2025-11-22 00Z
2025-12-22 00Z -> 2025-12-29 00Z
2026-01-01 00Z -> 2026-01-08 00Z
2026-02-08 00Z -> 2026-02-15 00Z
2026-03-15 00Z -> 2026-03-22 00Z
2026-04-22 00Z -> 2026-04-29 00Z
```

The staggered week-of-month pattern avoids always sampling only the first week
of each month.

Staged HRRR plans:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_pass_hrrr_monthly_202505_202604_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_hrrr_monthly_202505_202604_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_hrrr_monthly_202505_202604_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_hrrr_monthly_202505_202604_v1/plan.json
```

Monthly HRRR scale:

```text
4 domains x 84 domain-days x about 25 hourly rasters = about 8,400 natural-weather samples
4 domains x 84 chunks x 2 solvers = 672 WindNinja solver runs
```

Controlled forcing remains:

```text
11 speeds x 24 directions x 4 domains = 1,056 cases
2,112 mass/momentum controlled solver runs
```

Use this wrapper for the recommended robust monthly billable run:

```bash
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

This is now the preferred first robust data build: it keeps year-round HRRR,
adds balanced speed/direction forcing, and should fit a shorter cost cap than
the full-year-every-day HRRR batch.

Expected wall time for this first batch is roughly 18-30 hours if all four
domain workers stay healthy. Compute cost is likely tens of dollars on-demand
and lower on Spot, but Spot VMs can be preempted at any time. Use on-demand for
the first production run if you want the least operational friction.

Keep the VM and bucket in the same region. Cloud Storage Standard in
`us-central1` is cheap at this scale, and same-region storage keeps transfer
cost and latency low.

## One-VM GCP Runbook

Run this on one GCP VM. Do not run the full batch on the laptop.

Local shell setup:

```bash
export PROJECT_ID="your-gcp-project"
export ZONE="us-central1-a"
export REGION="us-central1"
export VM_NAME="mwn-ml-general-9p6"
export BUCKET="mwn-ml-general-9p6-yourname"

gcloud config set project "$PROJECT_ID"
gcloud services enable compute.googleapis.com storage.googleapis.com
```

Create a bucket in the same region:

```bash
gcloud storage buckets create "gs://${BUCKET}" \
  --location="$REGION" \
  --uniform-bucket-level-access
```

From this local repo, stage the non-git runtime assets for the VM. This is
important because the terrain files and generated run plans are ignored local
artifacts:

```bash
gcloud storage rsync -r static_data "gs://${BUCKET}/static_data"
gcloud storage rsync -r runtime/ml/residual_unet/hrrr_pairs "gs://${BUCKET}/hrrr_pairs"
```

Create the VM:

```bash
gcloud compute instances create "$VM_NAME" \
  --zone="$ZONE" \
  --machine-type="c4-standard-24" \
  --image-family="ubuntu-2204-lts" \
  --image-project="ubuntu-os-cloud" \
  --boot-disk-size="300GB" \
  --boot-disk-type="hyperdisk-balanced" \
  --scopes="cloud-platform"
```

SSH in:

```bash
gcloud compute ssh "$VM_NAME" --zone="$ZONE"
```

On the VM:

```bash
sudo apt-get update
sudo apt-get install -y git docker.io docker-compose-plugin tmux python3-venv
sudo usermod -aG docker "$USER"
exit
```

SSH back in so Docker group membership is active:

```bash
gcloud compute ssh "$VM_NAME" --zone="$ZONE"
```

On the VM, clone and prepare:

```bash
sudo mkdir -p /opt/mountain_windninja
sudo chown "$USER:$USER" /opt/mountain_windninja
git clone https://github.com/austfi/mountain_windninja.git /opt/mountain_windninja
cd /opt/mountain_windninja

python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r ml/residual_unet/requirements.txt

./deploy/gcp/mwn.sh init --image pull
./deploy/gcp/mwn.sh pull ghcr.io/austfi/mountain-windninja:3.12.2-herbie.3
docker ps
```

Bring this branch/state to the VM before the run. Prefer pushing a branch and
checking it out on the VM. If the work is not pushed yet, copy the changed files
to the VM before running. The VM must have:

```text
config/domains.json
ml/residual_unet/hrrr_pair_runs.py
ml/residual_unet/controlled_pairs.py
ml/residual_unet/build_controlled_dataset.py
runtime/ml/residual_unet/hrrr_pairs/*hrrr_monthly_202505_202604_v1/plan.json
runtime/ml/residual_unet/hrrr_pairs/*hrrr_monthly_202505_202604_v1/run_hrrr_pairs.sh
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_3plus1_hrrr_monthly_c4_24.sh
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
runtime/ml/residual_unet/raw/controlled_9p6_15deg/*/run_controlled_matrix.sh
runtime/ml/residual_unet/raw/controlled_9p6_15deg/run_3plus1_controlled_c4_24.sh
static_data/*.lcp for berthoud, breck, keystone, loveland_abasin
static_data/*.tif fallback DEMs for the same domains
```

Pull the generated runtime assets down from the bucket:

```bash
gcloud storage rsync -r "gs://${BUCKET}/static_data" static_data
gcloud storage rsync -r "gs://${BUCKET}/hrrr_pairs" runtime/ml/residual_unet/hrrr_pairs
chmod +x runtime/ml/residual_unet/hrrr_pairs/*/run_hrrr_pairs.sh
chmod +x runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/*.sh
chmod +x runtime/ml/residual_unet/raw/controlled_9p6_15deg/*.sh
chmod +x runtime/ml/residual_unet/raw/controlled_9p6_15deg/*/run_controlled_matrix.sh
```

Before the full batch, run the 24h data-only smokes:

```bash
tmux new -s mwn-smoke
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_smoke/run_hrrr_pairs.sh
runtime/ml/residual_unet/hrrr_pairs/keystone_9p6_smoke/run_hrrr_pairs.sh
runtime/ml/residual_unet/hrrr_pairs/loveland_abasin_9p6_smoke/run_hrrr_pairs.sh
```

Detach with `Ctrl-b`, then `d`. Reattach with:

```bash
tmux attach -t mwn-smoke
```

If both smokes pass, start the current monthly HRRR plus controlled batch:

```bash
tmux new -s mwn-monthly
MWN_ML_MAX_CONSECUTIVE_FAILURES=3 \
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

Monitor from another SSH session:

```bash
docker ps
docker stats
tail -f runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/logs/*.log
df -h .
```

Sync outputs to Cloud Storage after smokes and periodically during the full run:

```bash
gcloud storage rsync -r runtime/ml/residual_unet/hrrr_pairs "gs://${BUCKET}/hrrr_pairs"
gcloud storage rsync -r runtime/temp "gs://${BUCKET}/runtime_temp"
gcloud storage rsync -r static_data "gs://${BUCKET}/static_data"
```

After the batch and processed datasets are built, sync the Colab handoff
artifacts:

```bash
gcloud storage rsync -r ml/residual_unet/data/processed "gs://${BUCKET}/processed"
gcloud storage rsync -r ml/residual_unet/outputs/drive_upload "gs://${BUCKET}/drive_upload"
```

Stop the VM when idle:

```bash
exit
gcloud compute instances stop "$VM_NAME" --zone="$ZONE"
```

## Build Processed Datasets After Runs

After the raw mass/momentum runs are synced back, build one HRRR processed
dataset per domain:

```bash
.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/berthoud_pass_hrrr_monthly_202505_202604_v1 \
  --momentum-domain berthoud_pass \
  --mass-domain berthoud_pass_mass \
  --source-dataset berthoud_pass_hrrr_monthly_202505_202604_v1 \
  --sample-prefix berthoud_pass_hrrr_monthly_202505_202604_v1 \
  --force

.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/breck_tenmile_9p6_hrrr_monthly_202505_202604_v1 \
  --momentum-domain breck_tenmile_9p6 \
  --mass-domain breck_tenmile_9p6_mass \
  --source-dataset breck_tenmile_9p6_hrrr_monthly_202505_202604_v1 \
  --sample-prefix breck_tenmile_9p6_hrrr_monthly_202505_202604_v1 \
  --force

.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/keystone_9p6_hrrr_monthly_202505_202604_v1 \
  --momentum-domain keystone_9p6 \
  --mass-domain keystone_9p6_mass \
  --source-dataset keystone_9p6_hrrr_monthly_202505_202604_v1 \
  --sample-prefix keystone_9p6_hrrr_monthly_202505_202604_v1 \
  --force

.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/loveland_abasin_9p6_hrrr_monthly_202505_202604_v1 \
  --momentum-domain loveland_abasin_9p6 \
  --mass-domain loveland_abasin_9p6_mass \
  --source-dataset loveland_abasin_9p6_hrrr_monthly_202505_202604_v1 \
  --sample-prefix loveland_abasin_9p6_hrrr_monthly_202505_202604_v1 \
  --force
```

Then build one controlled processed dataset per domain. Pass
`--terrain-domain` explicitly so stale absolute terrain paths from another VM
or laptop do not affect terrain alignment:

```bash
.venv/bin/python -m ml.residual_unet.build_controlled_dataset \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/berthoud_pass \
  --out ml/residual_unet/data/processed/berthoud_pass_controlled_9p6_15deg \
  --terrain-domain berthoud_pass \
  --source-dataset berthoud_pass_controlled_9p6_15deg \
  --force

.venv/bin/python -m ml.residual_unet.build_controlled_dataset \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/breck_tenmile_9p6 \
  --out ml/residual_unet/data/processed/breck_tenmile_9p6_controlled_9p6_15deg \
  --terrain-domain breck_tenmile_9p6 \
  --source-dataset breck_tenmile_9p6_controlled_9p6_15deg \
  --force

.venv/bin/python -m ml.residual_unet.build_controlled_dataset \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/keystone_9p6 \
  --out ml/residual_unet/data/processed/keystone_9p6_controlled_9p6_15deg \
  --terrain-domain keystone_9p6 \
  --source-dataset keystone_9p6_controlled_9p6_15deg \
  --force

.venv/bin/python -m ml.residual_unet.build_controlled_dataset \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/loveland_abasin_9p6 \
  --out ml/residual_unet/data/processed/loveland_abasin_9p6_controlled_9p6_15deg \
  --terrain-domain loveland_abasin_9p6 \
  --source-dataset loveland_abasin_9p6_controlled_9p6_15deg \
  --force
```

Then combine the existing Berthoud V2 candidate with the new monthly HRRR and
controlled datasets:

```bash
.venv/bin/python -m ml.residual_unet.build_combined_dataset \
  --out ml/residual_unet/data/processed/mountain_general_9p6_monthly_controlled_v1 \
  --source berthoud_combined_v2=ml/residual_unet/data/processed/berthoud_combined_v2 \
  --source berthoud_pass_hrrr_monthly_202505_202604_v1=ml/residual_unet/data/processed/berthoud_pass_hrrr_monthly_202505_202604_v1 \
  --source breck_tenmile_9p6_hrrr_monthly_202505_202604_v1=ml/residual_unet/data/processed/breck_tenmile_9p6_hrrr_monthly_202505_202604_v1 \
  --source keystone_9p6_hrrr_monthly_202505_202604_v1=ml/residual_unet/data/processed/keystone_9p6_hrrr_monthly_202505_202604_v1 \
  --source loveland_abasin_9p6_hrrr_monthly_202505_202604_v1=ml/residual_unet/data/processed/loveland_abasin_9p6_hrrr_monthly_202505_202604_v1 \
  --source berthoud_pass_controlled_9p6_15deg=ml/residual_unet/data/processed/berthoud_pass_controlled_9p6_15deg \
  --source breck_tenmile_9p6_controlled_9p6_15deg=ml/residual_unet/data/processed/breck_tenmile_9p6_controlled_9p6_15deg \
  --source keystone_9p6_controlled_9p6_15deg=ml/residual_unet/data/processed/keystone_9p6_controlled_9p6_15deg \
  --source loveland_abasin_9p6_controlled_9p6_15deg=ml/residual_unet/data/processed/loveland_abasin_9p6_controlled_9p6_15deg \
  --force
```

Package the combined dataset for Colab:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/mountain_general_9p6_monthly_controlled_v1 \
  --skip-build \
  --force
```

## Colab Handoff

Upload the combined processed dataset zip and code zip to Drive or GCS. The
current first generalization package should be:

```text
ml/residual_unet/outputs/drive_upload/residual_unet_code.zip
ml/residual_unet/outputs/drive_upload/mountain_general_9p6_monthly_controlled_v1_dataset.zip
```

The training goal is not random held-out only; use domain-held-out evaluation:

```text
train on Berthoud + Breck + Keystone, hold out Loveland/A-Basin
train on Berthoud + Breck + Loveland/A-Basin, hold out Keystone
train on Berthoud + Keystone + Loveland/A-Basin, hold out Breck
```

Continue only if the model beats raw mass output on held-out terrain and beats
the current V1 Breck smoke result on the same completed Breck sample. Also
report HRRR-only and controlled-only held-out metrics separately; the controlled
matrix can look strong while HRRR weather cases remain weak, or vice versa.

For quick Colab smoke testing while a new GCP data-generation batch is still
running, refresh only the code/notebook handoff and reuse the existing dataset
ZIP already in the bucket:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --code-only \
  --gcs-bucket "${BUCKET}" \
  --notebook ml/residual_unet/notebooks/05_train_mountain_general_9p6_colab.ipynb
```

In Colab, set `SMOKE_TEST = True` in
`05_train_mountain_general_9p6_colab.ipynb`. That path downloads artifacts from
GCS to Colab local disk, runs a two-epoch capped-sample train/eval pass, writes
checkpoints and logs to Drive, and syncs the smoke result directory back to:

```text
gs://${BUCKET}/colab_results/<run_name>_smoke/
```

## References

- Google Compute Engine C4 machine specs: https://docs.cloud.google.com/compute/docs/general-purpose-machines
- Compute Engine pricing and Spot behavior: https://cloud.google.com/products/compute/pricing
- Spot VM preemption behavior: https://docs.cloud.google.com/compute/docs/instances/create-use-spot
- Cloud Storage pricing: https://cloud.google.com/storage/pricing

# Residual U-Net ML Offshoot

This is a research path for approximating WindNinja momentum-solver output from
the faster mass-solver output. It is intentionally separate from the operational
`mwn.sh` forecast and validation workflow.

## Current Model

The current best Berthoud model is `berthoud_combined_v1`.

Inputs:

```text
z_rel
dzdx
dzdy
u_mass
v_mass
```

The current LCP-canopy V2 experiment adds one simple LCP-derived channel:

```text
z_rel
dzdx
dzdy
canopy_cover
u_mass
v_mass
```

`canopy_cover` comes from LANDFIRE LCP band 5 (`LF2024_CC_CONUS`). This keeps
the first LCP experiment intentionally narrow: one canopy/roughness proxy, no
fuel-model category embeddings, and no canopy-height stack. Five-channel V1
datasets and six-channel LCP-canopy datasets cannot be mixed in one combined
training set.

Target:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

Prediction:

```text
u_corrected = u_mass + delta_u_pred
v_corrected = v_mass + delta_v_pred
```

## Inference Goal

The intended operational chain is:

```text
new HRRR data -> WindNinja mass solver -> residual U-Net inference -> momentum-like rasters
```

This avoids running the slow momentum/OpenFOAM solver for every forecast or
pastcast once the emulator is trusted for the target terrain and wind regimes.
It is still a momentum-solver emulator, not a direct observation-calibrated
truth model.

Apply the current best checkpoint to a completed mass-solver run:

```bash
.venv/bin/python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --mass-run runtime/temp/<berthoud_pass_mass_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph \
  --output-speed-units mph
```

When a paired momentum run exists, include it for a direct comparison:

```bash
.venv/bin/python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --mass-run runtime/temp/<berthoud_pass_mass_run> \
  --momentum-run runtime/temp/<berthoud_pass_momentum_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph
```

Inference writes the trained `96 x 96` center crop, not the full WindNinja
raster. Outputs include corrected speed/direction rasters, corrected `u/v`,
predicted residuals, `metadata.json`, and comparison metrics when a momentum run
is supplied.

## Current Results

Returned Colab artifacts are organized under:

```text
ml/residual_unet/colab/results/
```

The current best combined result is:

```text
ml/residual_unet/colab/results/berthoud_combined_v1/
```

Headline held-out metrics:

| Evaluation set | Mass vector RMSE | ML vector RMSE | Improvement |
|---|---:|---:|---:|
| All held-out samples | 7.385 | 1.958 | 73.5% |
| HRRR-derived only | 5.738 | 0.716 | 87.5% |
| Controlled only | 12.673 | 4.488 | 64.6% |

Interpretation: the combined model is the best current general Berthoud
momentum-emulator checkpoint. It still emulates WindNinja momentum output, not
observed atmospheric truth.

## Dataset Sources

`berthoud_v0`:

- HRRR-driven WindNinja mass/momentum pairs.
- Built from existing `runtime/temp/berthoud_pass*reanalysis*HRRR` artifacts.
- Current local processed coverage is 2026-01-01 00Z through 2026-04-01 00Z.

`controlled_berthoud_training`:

- Domain-average controlled speed/direction mass/momentum pairs.
- Speeds: 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80 mph.
- Directions: every 15 degrees.

`berthoud_combined_v1`:

- Merges the HRRR and controlled processed datasets.
- Evaluates all held-out samples and each source separately.

`berthoud_hrrr_oct_dec_2025_v1`:

- Built from salvaged paired HRRR mass/momentum outputs from the interrupted
  October-December 2025 run.
- Current processed coverage is 1,933 paired hourly samples.
- Keep this as a separate source label so V2 evaluation can report whether the
  new samples help without hiding source-specific failures.

`berthoud_combined_v2`:

- Merges `berthoud_v0`, `controlled_berthoud_training`, and
  `berthoud_hrrr_oct_dec_2025_v1`.
- Current local processed size is 4,358 samples: 3,454 train, 452 validation,
  and 452 test.
- This is a candidate training dataset. The current best checkpoint remains
  `berthoud_combined_v1` until the V2 Colab run returns better held-out and
  unseen-terrain metrics.

## Main Commands

Build the original HRRR dataset from existing paired outputs:

```bash
.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/berthoud_v0 \
  --crop-size 96 \
  --force
```

Build the controlled dataset after controlled WindNinja runs exist:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload --force
```

Build the combined dataset:

```bash
.venv/bin/python -m ml.residual_unet.build_combined_dataset --force
```

Build the salvaged Oct-Dec HRRR dataset from current paired outputs:

```bash
.venv/bin/python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/berthoud_hrrr_oct_dec_2025_v1 \
  --crop-size 96 \
  --source-dataset berthoud_hrrr_oct_dec_2025_v1 \
  --sample-prefix berthoud_hrrr_oct_dec_2025_v1 \
  --force
```

Build the V2 combined dataset:

```bash
.venv/bin/python -m ml.residual_unet.build_combined_dataset \
  --out ml/residual_unet/data/processed/berthoud_combined_v2 \
  --source berthoud_v0=ml/residual_unet/data/processed/berthoud_v0 \
  --source controlled_berthoud_training=ml/residual_unet/data/processed/controlled_berthoud_training \
  --source berthoud_hrrr_oct_dec_2025_v1=ml/residual_unet/data/processed/berthoud_hrrr_oct_dec_2025_v1 \
  --force
```

Package a processed dataset for Colab:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/berthoud_combined_v1 \
  --skip-build \
  --force
```

## Colab Notebooks

Use these notebooks in order:

```text
ml/residual_unet/notebooks/01_train_berthoud_v0_colab.ipynb
ml/residual_unet/notebooks/02_train_controlled_berthoud_colab.ipynb
ml/residual_unet/notebooks/03_train_berthoud_combined_colab.ipynb
```

Upload ZIP files to:

```text
MyDrive/windninja_ml/
```

For the combined run, upload:

```text
residual_unet_code.zip
berthoud_combined_v1_dataset.zip
```

For the V2 combined run, upload:

```text
residual_unet_code.zip
berthoud_combined_v2_dataset.zip
```

Then run:

```text
ml/residual_unet/notebooks/04_train_berthoud_combined_v2_colab.ipynb
```

For the four-domain mountain-general package, run:

```text
ml/residual_unet/notebooks/05_train_mountain_general_9p6_colab.ipynb
```

For the current LCP-canopy run, the GCS/Drive upload set is:

```text
residual_unet_code.zip
mountain_general_9p6_lcp_canopy_v1_dataset.zip
05_train_mountain_general_9p6_colab.ipynb
```

That notebook is the preferred Colab entrypoint for the current LCP-canopy
Berthoud/Breck-Keystone-Loveland generalization test. It defaults to
`mountain_general_9p6_lcp_canopy_v1` and now queues the two remaining
LCP-canopy holdouts:

```text
mountain_general_9p6_lcp_canopy_holdout_keystone_v1
mountain_general_9p6_lcp_canopy_holdout_breck_v1
```

The completed Loveland/A-Basin LCP-canopy baseline can be rerun by setting the
notebook `RUN_NAMES` list to:

```python
RUN_NAMES = ["mountain_general_9p6_lcp_canopy_holdout_loveland_v1"]
```

The notebook force-downloads and force-unpacks the code ZIP to avoid stale Colab
source files, reads ZIP artifacts from GCS directly onto Colab local disk, prints
the active CUDA device and dataset split counts, trains each held-out-terrain
config in `RUN_NAMES`, evaluates HRRR-only and controlled-only held-out sources
separately, and syncs each result directory back to GCS.
The training and evaluation cells call the Python functions directly inside the
notebook kernel so progress prints are visible in Colab; they do not launch a
buffered child process.

Default Colab training settings for this notebook:

```text
batch size: 32
DataLoader workers: 2
prefetch factor: 4
batch progress print: every 100 batches
```

If Colab runs out of GPU memory, lower the notebook `TRAIN_BATCH_SIZE` setting to
16 and rerun the training cell. The training command resumes from
`checkpoints/latest.pt` when that checkpoint exists.

For a cheap end-to-end Colab/GCS test before a full run, set this in the notebook
setup cell:

```python
SMOKE_TEST = True
```

Smoke mode writes to a separate `<run>_smoke` result directory, uses two epochs,
caps train/validation/evaluation samples, and still exercises GCS download,
local-disk unpacking, GPU training, held-out evaluation, and GCS result sync.

When only the code or notebook changed and the dataset ZIP already exists in the
bucket, refresh the GCS handoff without rebuilding a dataset ZIP:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --code-only \
  --gcs-bucket mwn-ml-general-9p6-spring-nova-475120-r0 \
  --notebook ml/residual_unet/notebooks/05_train_mountain_general_9p6_colab.ipynb
```

Build and package the six-channel LCP-canopy dataset from current four-domain
GCP outputs:

```bash
.venv/bin/python -m ml.residual_unet.build_mountain_general_lcp_canopy --force

.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/mountain_general_9p6_lcp_canopy_v1 \
  --skip-build \
  --force \
  --gcs-bucket mwn-ml-general-9p6-spring-nova-475120-r0 \
  --notebook ml/residual_unet/notebooks/05_train_mountain_general_9p6_colab.ipynb
```

The LCP-canopy builder creates one HRRR source and one controlled source per
domain, all with matching input channels. It intentionally does not include the
older `berthoud_combined_v2` seed because that seed is five-channel data.

Current Loveland/A-Basin held-out result from the mountain-general V1 Colab run
on 2026-05-19:

| Held-out source | Mass vector RMSE | ML vector RMSE | Improvement | Mass speed MAE | ML speed MAE |
|---|---:|---:|---:|---:|---:|
| Loveland/A-Basin HRRR monthly | 4.168 | 2.773 | 33.5% | 2.509 | 1.431 |
| Loveland/A-Basin controlled 15-degree | 12.867 | 9.145 | 28.9% | 7.127 | 4.685 |

Interpretation: this is the first useful unseen-terrain signal for the
four-domain 9.6 km residual U-Net. The HRRR-only Loveland result is the more
important operational check; it shows the model improved over the raw mass
solver on terrain withheld from training.

Current Loveland/A-Basin held-out result from the six-channel LCP-canopy Colab
run on 2026-05-22:

| Held-out source | Mass vector RMSE | ML vector RMSE | Improvement | Mass speed MAE | ML speed MAE |
|---|---:|---:|---:|---:|---:|
| Loveland/A-Basin HRRR LCP-canopy | 4.153 | 2.529 | 39.1% | 2.492 | 1.342 |
| Loveland/A-Basin controlled LCP-canopy 15-degree | 12.891 | 8.846 | 31.4% | 7.134 | 4.521 |

Interpretation: the simple LANDFIRE canopy-cover channel improved both
held-out Loveland/A-Basin HRRR and controlled evaluations relative to the
five-channel mountain-general V1 model. This is a useful signal, but it is not
enough by itself to call the emulator generally reliable. The next check is to
run the same LCP-canopy holdout workflow for Keystone and Breck/Tenmile and
compare source-specific held-out metrics before adding more LCP channels.

## Breckenridge/Tenmile Held-Out Terrain Check

The first unseen-terrain check is a 9.6 km Breckenridge/Tenmile box covering the
Breckenridge resort ridge segment from Peak 6 through Peak 10:

```text
domain: breck_tenmile_9p6
mass domain: breck_tenmile_9p6_mass
center: 39.4685, -106.0785
bbox N E S W: 39.51166738 -106.02258184 39.42533262 -106.13441816
```

Fetch/register terrain when Docker is available:

```bash
./deploy/gcp/mwn.sh fetch-terrain \
  --center 39.4685 -106.0785 \
  --size-km 9.6 \
  --domain breck_tenmile_9p6 \
  --label "Breckenridge Tenmile 9.6 km"
```

Run a 24-hour Breck smoke pair, then a 7-day pair window if the smoke is clean:

```bash
.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202601010000 \
  --end 202601020000 \
  --chunk-hours 24 \
  --threads 4 \
  --momentum-domain breck_tenmile_9p6 \
  --mass-domain breck_tenmile_9p6_mass \
  --label breck_tenmile_9p6_smoke \
  --infer-checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --terrain-domain breck_tenmile_9p6 \
  --write-run-script
```

Compare V1 and V2 checkpoints on the same Breck samples after V2 returns from
Colab. Treat V2 as promising only if it improves over raw mass output on Breck
and beats V1 on the same held-out terrain samples.

Current V1 Breck smoke result for `2026-01-01 00Z` through `2026-01-02 00Z`:

```text
samples: 25 hourly rasters
valid pixels: 228,000
mass vector RMSE: 4.291 m/s
V1 ML vector RMSE: 3.731 m/s
overall vector RMSE improvement: 13.0%
speed MAE: 2.284 m/s mass vs 1.982 m/s V1 ML
per-hour outcome: 20 improved, 5 worse
mean hourly improvement: 9.4%
median hourly improvement: 11.2%
best hour: 2026-01-01T14:00Z, 30.2% improvement
worst hour: 2026-01-01T06:00Z, 17.8% worse
```

Artifacts:

```text
runtime/ml/residual_unet/hrrr_pairs/breck_tenmile_9p6_smoke/breck_tenmile_9p6_bbox.kml
runtime/ml/residual_unet/inference/hrrr_pairs/breck_tenmile_9p6_smoke/20260101_0000_reanalysis_24h_HRRR/metrics.json
runtime/ml/residual_unet/inference/hrrr_pairs/breck_tenmile_9p6_smoke/20260101_0000_reanalysis_24h_HRRR/sample_metrics.csv
```

## Expanding HRRR Pair Data

More real HRRR mass/momentum pairs are the next practical way to improve the
model. Prepare a large no-run plan with:

The current no-Vail generalization plan is documented in:

```text
docs/ml_generalization_data_plan.md
```

The current active/preferred GCP batch is:

```text
runtime/ml/residual_unet/hrrr_pairs/general_9p6_monthly_202505_202604_v1/run_monthly_hrrr_plus_controlled_sync_and_stop.sh
```

It stages a one-VM GCP batch for Berthoud, Breck/Tenmile, Keystone, and
Loveland/A-Basin. It uses one staggered 7-day HRRR window per month from
May 2025 through April 2026, then adds 15-degree controlled speed/direction
forcing over the same four domains. This is data generation only; it does not
run ML inference during the expensive cloud solve.

After that run returns, build the processed datasets and combined Colab package
using the commands in `docs/ml_generalization_data_plan.md`. The expected
combined dataset name is:

```text
ml/residual_unet/data/processed/mountain_general_9p6_monthly_controlled_v1
```

For the simple LCP addition, use the six-channel successor:

```text
ml/residual_unet/data/processed/mountain_general_9p6_lcp_canopy_v1
```

Controlled processed datasets must be built per domain with
`ml.residual_unet.build_controlled_dataset --terrain-domain <domain>`, because
the raw controlled manifests can contain absolute terrain paths from the VM or
laptop that should not be reused for terrain alignment on another machine.

The older Berthoud-only planning command remains useful for small single-domain
experiments:

```bash
.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202510010000 \
  --end 202605010000 \
  --chunk-hours 24 \
  --threads 6 \
  --label berthoud_hrrr_20251001_20260501 \
  --write-run-script
```

To generate mass runs, true momentum runs, and ML-corrected momentum-like rasters
for the same chunks, include the current checkpoint:

```bash
.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202510010000 \
  --end 202605010000 \
  --chunk-hours 24 \
  --threads 6 \
  --label berthoud_hrrr_20251001_20260501_emulator \
  --infer-checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --write-run-script
```

This produces one 24-hour mass run, one 24-hour momentum run, and one ML
inference output directory per day. The ML step is cheap; the true momentum
solver remains the long-running part needed to expand the supervised training
target and validation set.

This writes a plan and runner under:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501/
```

The generated runner skips chunks that already appear complete. It should only
be run when no other WindNinja/OpenFOAM job is active.

## Thread Benchmark

A small momentum-only benchmark is prepared under:

```text
runtime/ml/residual_unet/thread_benchmark/
```

Run it only when no other WindNinja/OpenFOAM container is active:

```bash
./runtime/ml/residual_unet/thread_benchmark/run_momentum_thread_benchmark.sh
```

Use the result to decide whether the large HRRR pair plan should remain at six
threads or be regenerated at four threads.

## Cleanup Boundary

Safe generated ML cleanup targets:

```text
ml/residual_unet/data/processed/
ml/residual_unet/outputs/
ml/residual_unet/colab/
runtime/ml/residual_unet/
```

Do not clean `runtime/ml/residual_unet/` or `runtime/temp/` while the GCP
monthly data-generation run is active or before its GCS sync has been verified.
Those paths contain the raw paired outputs needed to build the next training
dataset.

Do not delete these unless you are intentionally discarding local results:

```text
ml/residual_unet/colab/results/*/best.pt
ml/residual_unet/colab/results/*/metrics.json
runtime/temp/berthoud_pass*reanalysis*HRRR
```

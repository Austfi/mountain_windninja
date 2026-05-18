# WindNinja Residual U-Net

This is an isolated ML offshoot for learning a correction from WindNinja's fast
mass solver toward its slower mass-and-momentum solver. The first checkpoint is
Berthoud-only; the current data-build branch is expanding the training set to
Berthoud, Breck/Tenmile, Keystone, and Loveland/A-Basin 9.6 km boxes. It is
intentionally adjacent to the operational wrapper code and reads completed
WindNinja artifacts under `runtime/` and terrain under `static_data/`.

For the current status, results, and cleanup boundary, see
[`docs/ml_residual_unet.md`](../../docs/ml_residual_unet.md).

## V0 Question

Can a small terrain-aware U-Net learn:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

well enough that:

```text
RMSE(mass_uv + predicted_delta_uv, momentum_uv)
  < RMSE(mass_uv, momentum_uv)
```

## HRRR Dataset

The first dataset builder pairs Berthoud Pass HRRR run folders:

```text
runtime/temp/berthoud_pass_*_reanalysis_24h_HRRR
runtime/temp/berthoud_pass_mass_*_reanalysis_24h_HRRR
```

It converts speed/direction rasters to `u/v`, creates terrain channels aligned to
the WindNinja output grid, center-crops samples to `96 x 96`, and writes NPZ
shards under `ml/residual_unet/data/processed/`.

Generated data, checkpoints, metrics, and figures are ignored by git.

## Local Setup

Install the ML-only dependencies in a separate environment when you are working
on this path:

```bash
python -m pip install -r ml/residual_unet/requirements.txt
```

Build the v0 dataset:

```bash
python -m ml.residual_unet.build_dataset \
  --source-root . \
  --out ml/residual_unet/data/processed/berthoud_v0 \
  --crop-size 96
```

Run a tiny local training smoke test:

```bash
python -m ml.residual_unet.train \
  --config ml/residual_unet/configs/berthoud_v0.yaml \
  --epochs 1 \
  --max-train-samples 8 \
  --max-val-samples 4
```

Evaluate a checkpoint:

```bash
python -m ml.residual_unet.evaluate \
  --checkpoint ml/residual_unet/outputs/checkpoints/best.pt \
  --data ml/residual_unet/data/processed/berthoud_v0 \
  --out ml/residual_unet/outputs/eval/berthoud_v0
```

## Inference on a Mass-Solver Run

Use the current best checkpoint to turn completed mass-solver rasters into
momentum-like corrected rasters:

```bash
python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --mass-run runtime/temp/<berthoud_pass_mass_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph \
  --output-speed-units mph
```

The first inference version writes the model's trained `96 x 96` center crop.
It writes corrected speed/direction rasters, corrected `u/v` rasters, residual
`delta_u/delta_v` rasters, and `metadata.json`.

If a paired momentum run exists, add it to produce comparison metrics:

```bash
python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/berthoud_combined_v1/best.pt \
  --mass-run runtime/temp/<berthoud_pass_mass_run> \
  --momentum-run runtime/temp/<berthoud_pass_momentum_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph
```

That comparison writes `metrics.json` and `sample_metrics.csv` showing whether
the ML-corrected field is closer to momentum output than raw mass output.

## Folder Layout

Keep source code and generated experiment artifacts separate:

```text
ml/residual_unet/
  configs/        committed training configs
  notebooks/      committed Colab wrappers
  data/processed/ generated local datasets, git-ignored
  outputs/        generated ZIPs, checkpoints, metrics, git-ignored
  colab/results/ returned Colab artifacts kept for inspection, git-ignored
```

## Colab

Use `notebooks/01_train_berthoud_v0_colab.ipynb` as a thin Colab Pro wrapper.
Store compressed dataset artifacts and checkpoints in Google Drive, copy the
dataset to Colab local disk before training, and save `latest.pt` and `best.pt`
back to Drive each epoch.

## Controlled Wind Cases

After the HRRR-pair model works, generate controlled mass/momentum pairs to test
whether the correction generalizes beyond nearby HRRR cases.

Plan the main training matrix without running WindNinja:

```bash
.venv/bin/python -m ml.residual_unet.controlled_pairs --profile training --plan
```

The training matrix uses 15-degree directions and wind speeds from 5 to 80 mph:

```text
11 speeds x 24 directions x 2 solvers = 528 WindNinja runs
speed bins: 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80 mph
```

Write configs and a manifest under `runtime/ml/residual_unet/raw/`:

```bash
.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile training \
  --write-configs \
  --write-run-script
```

Run the generated training matrix only when no other WindNinja/OpenFOAM job is
active. The generated runner opens one long-lived container from
`MWN_DOCKER_IMAGE` in `config/runtime.env`; it does not use Compose and does not
trigger a local image build.

```bash
./runtime/ml/residual_unet/raw/controlled_berthoud_training/run_controlled_matrix.sh
```

The smaller standard matrix remains available for quick runs:

```text
5 speeds x 12 directions x 2 solvers = 120 WindNinja runs
```

```bash
.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile standard \
  --plan
```

For a smaller execution check:

```bash
.venv/bin/python -m ml.residual_unet.controlled_pairs \
  --profile pilot \
  --write-configs \
  --write-run-script \
  --max-runs 2 \
  --raw-root runtime/ml/residual_unet/raw/controlled_berthoud_smoke

./runtime/ml/residual_unet/raw/controlled_berthoud_smoke/run_controlled_matrix.sh
```

After completed controlled runs exist, build the controlled NPZ dataset:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --force
```

That command builds:

```text
ml/residual_unet/data/processed/controlled_berthoud_training/
```

and writes Colab upload ZIPs:

```text
ml/residual_unet/outputs/drive_upload/residual_unet_code.zip
ml/residual_unet/outputs/drive_upload/controlled_berthoud_training_dataset.zip
```

Upload both ZIP files to:

```text
MyDrive/windninja_ml/
```

Then run:

```text
ml/residual_unet/notebooks/02_train_controlled_berthoud_colab.ipynb
```

For the current four-domain 9.6 km generalization run, build controlled
processed datasets per domain instead of using the Berthoud-only default:

```bash
.venv/bin/python -m ml.residual_unet.build_controlled_dataset \
  --raw-root runtime/ml/residual_unet/raw/controlled_9p6_15deg/keystone_9p6 \
  --out ml/residual_unet/data/processed/keystone_9p6_controlled_9p6_15deg \
  --terrain-domain keystone_9p6 \
  --source-dataset keystone_9p6_controlled_9p6_15deg \
  --force
```

Use the same pattern for `berthoud_pass`, `breck_tenmile_9p6`, and
`loveland_abasin_9p6`. Passing `--terrain-domain` avoids reusing stale absolute
terrain paths from a different machine.

## Combined HRRR + Controlled Run

After both individual runs work, merge the existing HRRR-derived Berthoud dataset
and the controlled Berthoud dataset:

```bash
.venv/bin/python -m ml.residual_unet.build_combined_dataset --force
```

That writes:

```text
ml/residual_unet/data/processed/berthoud_combined_v1/
```

Package the combined dataset for Colab:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/berthoud_combined_v1 \
  --skip-build \
  --force
```

Upload these to `MyDrive/windninja_ml/`:

```text
ml/residual_unet/outputs/drive_upload/residual_unet_code.zip
ml/residual_unet/outputs/drive_upload/berthoud_combined_v1_dataset.zip
```

Then run:

```text
ml/residual_unet/notebooks/03_train_berthoud_combined_colab.ipynb
```

The combined notebook evaluates the same checkpoint three ways:

```text
all held-out samples
HRRR-derived held-out dates only
controlled held-out directions only
```

## Expanding HRRR Paired Data

Use `hrrr_pair_runs.py` to prepare paired HRRR mass/momentum runs without
starting the solver immediately.

Example: prepare an October 2025 through April 2026 plan in 24-hour chunks using
six threads:

```bash
.venv/bin/python -m ml.residual_unet.hrrr_pair_runs \
  --start 202510010000 \
  --end 202605010000 \
  --chunk-hours 24 \
  --threads 6 \
  --label berthoud_hrrr_20251001_20260501 \
  --write-run-script
```

To also run the current ML momentum emulator after each completed mass/momentum
chunk, add the checkpoint:

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

This writes:

```text
runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501/plan.json
runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501/run_hrrr_pairs.sh
```

The generated runner skips chunks that already have enough WindNinja ASCII output
and skips ML inference chunks that already have `metadata.json`. It does not
delete `runtime/temp/` on failure. Run it only when you are ready to spend CPU
time:

```bash
./runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501/run_hrrr_pairs.sh
```

For the emulator-labeled plan, run:

```bash
./runtime/ml/residual_unet/hrrr_pairs/berthoud_hrrr_20251001_20260501_emulator/run_hrrr_pairs.sh
```

ML-corrected rasters and comparison metrics are written under:

```text
runtime/ml/residual_unet/inference/hrrr_pairs/berthoud_hrrr_20251001_20260501_emulator/
```

The default domains are:

```text
berthoud_pass       momentum solver
berthoud_pass_mass  mass solver
```

## Momentum Thread Benchmark

A small domain-average momentum benchmark can compare 4 vs 6 OpenFOAM threads
without running the full HRRR matrix. The prepared benchmark uses one 40 mph,
270-degree Berthoud Pass momentum case for each thread count.

Run only when no other WindNinja/OpenFOAM container is active:

```bash
./runtime/ml/residual_unet/thread_benchmark/run_momentum_thread_benchmark.sh
```

The timing logs are written to:

```text
runtime/ml/residual_unet/thread_benchmark/momentum_4_threads_time.log
runtime/ml/residual_unet/thread_benchmark/momentum_6_threads_time.log
```

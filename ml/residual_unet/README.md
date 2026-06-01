# WindNinja Residual U-Net

This is an isolated ML offshoot for learning a correction from WindNinja's fast
mass solver toward its slower mass-and-momentum solver. It is adjacent to the
operational `mwn.sh` workflow and should not change normal WindNinja forecast or
validation behavior.

Canonical status, results, and operating notes live in
[`docs/ml_residual_unet.md`](../../docs/ml_residual_unet.md).

## Current Direction

The leading practical experiment is terrain-specific momentum emulation for
fixed 9.6 km boxes:

```text
breck_tenmile_9p6_specific_lcp_canopy_v2
keystone_9p6_specific_lcp_canopy_v2
```

The model learns:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

so inference produces:

```text
u_ml = u_mass + predicted_delta_u
v_ml = v_mass + predicted_delta_v
```

Current six-channel LCP-canopy inputs:

```text
z_rel
dzdx
dzdy
canopy_cover
u_mass
v_mass
```

Older Berthoud-only runs used five channels without `canopy_cover`; do not mix
five-channel and six-channel sources in one combined dataset.

## Latest Results

Latest site-specific held-out test results:

| Model | Mass RMSE | ML RMSE | Improvement | ML better pixels |
|---|---:|---:|---:|---:|
| Breck/Tenmile V2 overall | 5.181 m/s | 1.054 m/s | 79.7% | 95.1% |
| Breck/Tenmile V2 HRRR only | 3.697 m/s | 0.577 m/s | 84.4% | 95.6% |
| Keystone V2 overall | 4.271 m/s | 1.164 m/s | 72.7% | 95.7% |
| Keystone V2 HRRR only | 2.815 m/s | 0.421 m/s | 85.0% | 96.5% |

Controlled 15-degree stress tests improve strongly but remain harder:

| Model | Controlled mass RMSE | Controlled ML RMSE | Improvement |
|---|---:|---:|---:|
| Breck/Tenmile V2 15-degree | 12.409 m/s | 2.881 m/s | 76.8% |
| Breck/Tenmile V2 midpoint | 12.589 m/s | 3.034 m/s | 75.9% |
| Keystone V2 15-degree | 10.871 m/s | 3.543 m/s | 67.4% |
| Keystone V2 midpoint | 10.993 m/s | 3.645 m/s | 66.8% |

V2 independently evaluates held-out 7.5-degree midpoint directions. The HRRR
operational score improved slightly versus V1, while controlled/high-wind stress
cases are the remaining gap.

The completed `v2_gradloss` ablation did not replace V2 as the champion. It was
slightly worse for Breck overall and only marginally better for Keystone
overall, so the next experiment prioritizes real HRRR rows and architecture
capacity instead of optimizing controlled outliers.

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

Generated data, checkpoints, metrics, and figures are ignored by git.

## Colab

Use the site-specific notebook for current Breck/Keystone work:

```text
ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb
```

The notebook can reproduce the completed V2 baseline and gradloss ablation, but
now defaults to HRRR-priority architecture ablations. These runs train/validate
on HRRR rows only, then still evaluate controlled rows as stress tests.

Current GCS handoff artifacts:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/residual_unet_code.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/06_train_site_specific_9p6_colab.ipynb
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v1_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v1_dataset.zip
```

The current stricter package writes V2 artifacts:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v2_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v2_dataset.zip
```

The completed gradloss ablation wrote:

```text
breck_tenmile_9p6_specific_lcp_canopy_v2_gradloss
keystone_9p6_specific_lcp_canopy_v2_gradloss
```

The next HRRR-priority architecture ablations write:

```text
breck_tenmile_9p6_specific_lcp_canopy_v2_hrrr_only
breck_tenmile_9p6_specific_lcp_canopy_v2_hrrr_unet64
breck_tenmile_9p6_specific_lcp_canopy_v2_hrrr_resunet32
keystone_9p6_specific_lcp_canopy_v2_hrrr_only
keystone_9p6_specific_lcp_canopy_v2_hrrr_unet64
keystone_9p6_specific_lcp_canopy_v2_hrrr_resunet32
```

Results sync back to:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/colab_results/
```

Read these first:

```text
scorecard/scorecard_report.md
scorecard/scorecard_summary.json
eval/<source_dataset>/metrics.json
train_log.csv
_comparison/comparison_report.md
```

If the notebook is replaced in GCS, close the old Colab tab and reopen the Drive
copy. Colab can keep a stale browser copy open after the Drive file changes.

## Local Setup

Install the ML-only dependencies in a separate environment:

```bash
python -m pip install -r ml/residual_unet/requirements.txt
```

Run a tiny local training smoke test when a processed dataset exists:

```bash
python -m ml.residual_unet.train \
  --config ml/residual_unet/configs/breck_tenmile_9p6_specific_lcp_canopy_v1.yaml \
  --epochs 1 \
  --max-train-samples 8 \
  --max-val-samples 4
```

Evaluate a checkpoint:

```bash
python -m ml.residual_unet.evaluate \
  --checkpoint ml/residual_unet/outputs/checkpoints/best.pt \
  --data ml/residual_unet/data/processed/breck_tenmile_9p6_specific_lcp_canopy_v1 \
  --out ml/residual_unet/outputs/eval/breck_tenmile_9p6_specific_lcp_canopy_v1
```

Write a terrain-specific scorecard:

```bash
python -m ml.residual_unet.emulator_scorecard \
  --checkpoint ml/residual_unet/outputs/checkpoints/best.pt \
  --data ml/residual_unet/data/processed/breck_tenmile_9p6_specific_lcp_canopy_v1 \
  --out ml/residual_unet/outputs/scorecard/breck_tenmile_9p6_specific_lcp_canopy_v1 \
  --split test \
  --batch-size 32
```

## Building Site-Specific Datasets

After raw HRRR and controlled mass/momentum outputs exist:

```bash
.venv/bin/python -m ml.residual_unet.build_domain_specific_lcp_canopy \
  --domain breck \
  --force

.venv/bin/python -m ml.residual_unet.build_domain_specific_lcp_canopy \
  --domain keystone \
  --force
```

Site package definitions are stored in:

```text
ml/residual_unet/configs/site_specific_9p6_lcp_canopy.json
```

Add future terrain boxes there and reuse the same builder with
`--domain <site-key>`. The spec also controls midpoint controlled validation
and test direction holdouts.

The current default site spec writes V2 dataset names. V1 is the existing
baseline; V2 is the rebuild with independent midpoint controlled holdouts.

Package a processed dataset and notebook for Colab:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/breck_tenmile_9p6_specific_lcp_canopy_v2 \
  --skip-build \
  --force \
  --gcs-bucket mwn-ml-general-9p6-spring-nova-475120-r0 \
  --notebook ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb
```

The tracked GCP wrapper for the full Breck/Keystone data build is:

```bash
ml/residual_unet/run_breck_keystone_specific_data_build_gcp.sh
```

It runs Breck and Keystone HRRR pairs, controlled midpoint pairs, packaging,
GCS sync, and VM shutdown. The current completed package used 362 good HRRR
days after skipping three repeatedly failing dates.

If the raw HRRR and controlled outputs are already in GCS, prefer the
package-only V2 wrapper:

```bash
ml/residual_unet/package_breck_keystone_specific_v2_gcp.sh
```

On a GCP VM, run it with `SHUTDOWN_ON_COMPLETE=1` to shut down after packaging.

## Inference on a Mass-Solver Run

Use a trained checkpoint to turn completed mass-solver rasters into momentum-like
corrected rasters:

```bash
python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/<run_name>/best.pt \
  --mass-run runtime/temp/<mass_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph \
  --output-speed-units mph
```

If a paired momentum run exists, add it to produce comparison metrics:

```bash
python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/<run_name>/best.pt \
  --mass-run runtime/temp/<mass_run> \
  --momentum-run runtime/temp/<momentum_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph
```

Current inference writes the trained `96 x 96` center crop, not a full-domain
raster.

## Older Baselines

Older notebooks remain for reproducibility:

```text
01_train_berthoud_v0_colab.ipynb
02_train_controlled_berthoud_colab.ipynb
03_train_berthoud_combined_colab.ipynb
04_train_berthoud_combined_v2_colab.ipynb
05_train_mountain_general_9p6_colab.ipynb
```

The older five-channel `berthoud_combined_v1` checkpoint remains a useful
Berthoud baseline, but the practical current direction is the six-channel
site-specific Breck/Keystone path.

# Residual U-Net ML Offshoot

This is a research path for approximating WindNinja momentum-solver output from
the faster mass-solver output. It is intentionally separate from the operational
`mwn.sh` forecast and validation workflow.

The current practical direction is **terrain-specific momentum emulation** for
fixed 9.6 km terrain boxes. The general mountain model remains useful research,
but current evidence says the small residual U-Net is strongest when trained and
evaluated on the same terrain box.

## Current Recommendation

As of the latest completed Colab/GCS results, use the site-specific Breck and
Keystone models as the leading experiment:

```text
breck_tenmile_9p6_specific_lcp_canopy_v1
keystone_9p6_specific_lcp_canopy_v1
```

Primary question:

```text
Can mass solver + residual U-Net emulate the full WindNinja momentum solver
closely enough for this exact 9.6 km terrain box?
```

This is not observation calibration. The target is the WindNinja momentum solver:

```text
delta_u = u_momentum - u_mass
delta_v = v_momentum - v_mass
```

Prediction:

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

`canopy_cover` comes directly from LANDFIRE LCP band 5 (`LF2024_CC_CONUS`).
Do not combine older five-channel datasets with these six-channel datasets in
one training set.

## Latest Site-Specific Results

The latest completed site-specific Colab run trained both Breck/Tenmile and
Keystone for 100 epochs and synced results to:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/colab_results/
```

### Training Validation

| Model | Best validation epoch | Validation ML vector RMSE | Validation mass vector RMSE |
|---|---:|---:|---:|
| Breck/Tenmile site-specific | 97 | 0.641 m/s | 3.964 m/s |
| Keystone site-specific | 97 | 0.552 m/s | 3.066 m/s |

### Held-Out Test Summary

| Model | Sources | Mass RMSE | ML RMSE | Improvement | ML better pixels |
|---|---:|---:|---:|---:|---:|
| Breck/Tenmile site-specific | 3 | 4.524 m/s | 0.806 m/s | 82.2% | 95.4% |
| Keystone site-specific | 3 | 3.642 m/s | 0.775 m/s | 78.7% | 96.2% |
| Mountain-general LCP-canopy all-domain | 8 | 5.587 m/s | 1.511 m/s | 73.0% | 95.2% |

The site-specific models are now the best current models for Breck and Keystone
same-terrain emulation.

### HRRR-Only Operational Score

HRRR test samples are the most important operational score because they reflect
realistic weather cases for the same terrain.

| Model | HRRR mass RMSE | HRRR ML RMSE | Improvement | ML better pixels | ML <=1 m/s | ML <=2 m/s |
|---|---:|---:|---:|---:|---:|---:|
| Breck/Tenmile | 3.697 m/s | 0.626 m/s | 83.1% | 95.6% | 93.1% | 99.2% |
| Keystone | 2.815 m/s | 0.452 m/s | 83.9% | 96.4% | 97.0% | 99.8% |

Interpretation: on realistic same-terrain HRRR cases, the ML field is usually
about 1.0-1.4 mph vector error away from the full momentum solve.

### Controlled 15-Degree Stress Test

Controlled cases force a broad speed/direction matrix and are intentionally
harder than typical HRRR weather.

| Model | Controlled mass RMSE | Controlled ML RMSE | Improvement | ML better pixels | ML <=1 m/s | ML <=2 m/s |
|---|---:|---:|---:|---:|---:|---:|
| Breck/Tenmile | 12.409 m/s | 2.392 m/s | 80.7% | 91.1% | 48.3% | 75.7% |
| Keystone | 10.871 m/s | 2.897 m/s | 73.4% | 91.0% | 44.0% | 72.2% |

Interpretation: the models strongly improve controlled cases, but controlled
edge cases are still not as close as HRRR cases. Treat them as stress tests for
weak directions and high speeds.

### Important Midpoint-Controlled Caveat

The 7.5-degree midpoint controlled matrix was included in training, but the
current packaged split has it as train-only:

```text
controlled_9p6_7p5_midpoints: train=264, val=0, test=0 per domain
```

That data can help the model learn between the 15-degree directions, but the
latest results do not independently evaluate midpoint controlled cases. The next
dataset version should reserve midpoint controlled cases for validation/test.
The builder now supports this explicitly through configurable controlled
direction holdouts; the default site-specific spec holds out midpoint directions
`37.5, 127.5, 217.5, 307.5` for validation and `67.5, 157.5, 247.5, 337.5`
for test when those midpoint raw cases are rebuilt.

## Packaged Data

The current packaged Colab inputs are:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/residual_unet_code.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/06_train_site_specific_9p6_colab.ipynb
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v1_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v1_dataset.zip
```

The next validation-quality rebuild should write separate V2 artifacts so the
current V1 results remain comparable:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/breck_tenmile_9p6_specific_lcp_canopy_v2_dataset.zip
gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/keystone_9p6_specific_lcp_canopy_v2_dataset.zip
```

Dataset composition:

| Dataset | Total samples | HRRR samples | 15-degree controlled | 7.5-degree midpoint controlled |
|---|---:|---:|---:|---:|
| Breck/Tenmile | 9,219 | 8,691 | 264 | 264 |
| Keystone | 9,218 | 8,690 | 264 | 264 |

The full-year HRRR run completed 362 of 365 daily chunks. These three dates were
skipped after repeated HRRR/WindNinja failures:

```text
2025-06-27
2025-11-20
2025-12-14
```

Those skips are acceptable for the current training package. The original GCP
wrapper exits nonzero and skips packaging when any HRRR chunk fails; the finished
package was created by a package-only repair after restoring the 15-degree
controlled raw data from GCS.

## Colab Workflow

Use this notebook for the current site-specific training path:

```text
ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb
```

The notebook now targets the V2 package by default:

```text
breck_tenmile_9p6_specific_lcp_canopy_v2
keystone_9p6_specific_lcp_canopy_v2
```

In Colab, use a GPU runtime. L4 is sufficient; A100/H100 is faster but not
required. Do not use CPU or TPU for the full run.

Pull the current notebook into Drive:

```python
from google.colab import auth, drive
auth.authenticate_user()
drive.mount('/content/drive')

!mkdir -p /content/drive/MyDrive/windninja_ml
!gcloud config set project spring-nova-475120-r0
!gcloud storage cp gs://mwn-ml-general-9p6-spring-nova-475120-r0/drive_upload/06_train_site_specific_9p6_colab.ipynb /content/drive/MyDrive/windninja_ml/06_train_site_specific_9p6_colab.ipynb
```

If the notebook was already open when the GCS object changed, close the stale
Colab tab and reopen the Drive copy. Colab can keep serving an old in-browser
copy even after the Drive file is replaced.

The notebook:

- downloads code and dataset ZIPs from GCS to Colab local disk
- unpacks datasets under `/content/data`
- trains Breck and Keystone as separate runs
- writes `best.pt`, `latest.pt`, and `train_log.csv`
- evaluates each source dataset separately
- writes emulator scorecards
- writes a cross-run comparison under `_comparison`
- syncs results back to GCS

Default training settings:

```text
epochs: 100
batch size: 32
DataLoader workers: 2
prefetch factor: 4
progress print: every 100 batches
```

If Colab runs out of GPU memory, lower `TRAIN_BATCH_SIZE` to 16 and rerun the
training cell. The notebook resumes from `checkpoints/latest.pt` when present.

## Result Files To Read First

For each run:

```text
MyDrive/windninja_ml/results/<run_name>/scorecard/scorecard_report.md
MyDrive/windninja_ml/results/<run_name>/scorecard/scorecard_summary.json
MyDrive/windninja_ml/results/<run_name>/eval/<source_dataset>/metrics.json
MyDrive/windninja_ml/results/<run_name>/train_log.csv
```

GCS mirror:

```text
gs://mwn-ml-general-9p6-spring-nova-475120-r0/colab_results/<run_name>/
gs://mwn-ml-general-9p6-spring-nova-475120-r0/colab_results/_comparison/
```

The cross-run comparison is generated by `ml.residual_unet.compare_results` and
writes:

```text
comparison_metrics.csv
comparison_run_summary.csv
comparison_summary.json
comparison_report.md
```

Use `comparison_report.md` to compare model progression. Use the per-run
`scorecard_report.md` to decide whether a model is operationally credible for a
specific terrain.

## Evaluation Metrics

Core momentum-emulator metrics:

- `ml_vector_rmse`: vector error from momentum after ML correction
- `mass_vector_rmse`: vector error from momentum before ML correction
- `vector_rmse_improvement_percent`: percent improvement vs mass
- `ml_speed_mae`: speed error from momentum after ML correction
- `ml_better_pixel_fraction`: fraction of valid pixels where ML is closer to
  momentum than mass is

Close/off pixel metrics:

```text
ml_vector_error_le_0p5mps_fraction
ml_vector_error_le_1p0mps_fraction
ml_vector_error_le_2p0mps_fraction
ml_vector_error_le_3p0mps_fraction
ml_vector_error_le_5p0mps_fraction
ml_better_by_1mps_pixel_fraction
ml_worse_by_1mps_pixel_fraction
```

Scorecard stratifications:

- HRRR vs controlled source kind
- season and month
- 45-degree direction sector
- target momentum speed bins
- high-wind target pixels at >=10 m/s
- canopy-cover bins
- lee, windward, and cross/flat slope bins
- worst held-out samples
- worst regressions

Decision boundary:

- HRRR-only same-terrain held-out performance is the primary practical score.
- Controlled-only performance is a direction/speed stress test.
- A low aggregate RMSE is not enough if high-wind, lee-side, or direction-sector
  rows show large regressions.

## Data Generation Workflow

The tracked GCP wrapper for the Breck/Keystone site-specific data build is:

```bash
ml/residual_unet/run_breck_keystone_specific_data_build_gcp.sh
```

If the existing HRRR and controlled raw outputs are already in GCS, use the
package-only V2 wrapper first. It restores `static_data`, `runtime_temp`, and
`runtime_ml`, rebuilds the processed datasets with the midpoint controlled
validation/test split, uploads the V2 dataset ZIPs and notebook, and syncs logs:

```bash
ml/residual_unet/package_breck_keystone_specific_v2_gcp.sh
```

On a GCP VM, add `SHUTDOWN_ON_COMPLETE=1` if the VM should shut down after the
package step:

```bash
SHUTDOWN_ON_COMPLETE=1 ml/residual_unet/package_breck_keystone_specific_v2_gcp.sh
```

It restores static terrain from GCS, optionally restores previous `runtime/temp`
outputs, stages HRRR and controlled runners, runs Breck and Keystone in
parallel, builds domain-specific datasets, uploads Colab artifacts, syncs
runtime outputs, and shuts the VM down.

Full-year HRRR plan labels:

```text
breck_tenmile_9p6_hrrr_20250501_20260501_v1
keystone_9p6_hrrr_20250501_20260501_v1
```

Controlled raw roots:

```text
runtime/ml/residual_unet/raw/controlled_9p6_15deg/<domain>/
runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints/<domain>/
```

Build a site-specific processed dataset after raw mass/momentum outputs exist:

```bash
.venv/bin/python -m ml.residual_unet.build_domain_specific_lcp_canopy \
  --domain breck \
  --force

.venv/bin/python -m ml.residual_unet.build_domain_specific_lcp_canopy \
  --domain keystone \
  --force
```

The site definitions live in:

```text
ml/residual_unet/configs/site_specific_9p6_lcp_canopy.json
```

Add a future terrain box there first, then run the same builder with
`--domain <site-key>`. That keeps future terrain packages config-driven instead
of adding another hardcoded Python branch.

The current default site spec writes V2 dataset names. V1 remains the existing
baseline result; V2 is the stricter midpoint-holdout rebuild.

Package a processed dataset for Colab:

```bash
.venv/bin/python -m ml.residual_unet.prepare_colab_upload \
  --processed-dir ml/residual_unet/data/processed/breck_tenmile_9p6_specific_lcp_canopy_v1 \
  --skip-build \
  --force \
  --gcs-bucket mwn-ml-general-9p6-spring-nova-475120-r0 \
  --notebook ml/residual_unet/notebooks/06_train_site_specific_9p6_colab.ipynb
```

## Historical Generalization Results

Older Berthoud and mountain-general runs remain useful baselines.

### Berthoud Combined V1

Five-channel Berthoud-only combined model:

```text
z_rel, dzdx, dzdy, u_mass, v_mass
```

| Evaluation set | Mass vector RMSE | ML vector RMSE | Improvement |
|---|---:|---:|---:|
| All held-out samples | 7.385 m/s | 1.958 m/s | 73.5% |
| HRRR-derived only | 5.738 m/s | 0.716 m/s | 87.5% |
| Controlled only | 12.673 m/s | 4.488 m/s | 64.6% |

### Mountain-General LCP-Canopy Holdouts

Six-channel four-domain LCP-canopy model holdout results:

| Held-out source | Mass vector RMSE | ML vector RMSE | Improvement |
|---|---:|---:|---:|
| Loveland/A-Basin HRRR | 4.153 m/s | 2.529 m/s | 39.1% |
| Loveland/A-Basin controlled | 12.891 m/s | 8.846 m/s | 31.4% |
| Keystone HRRR | 2.878 m/s | 3.211 m/s | -11.6% |
| Keystone controlled | 10.871 m/s | 9.454 m/s | 13.0% |
| Breck/Tenmile HRRR | 3.981 m/s | 2.775 m/s | 30.3% |
| Breck/Tenmile controlled | 12.409 m/s | 8.264 m/s | 33.4% |

Interpretation: the general model improves some unseen terrain but not all of
it. Keystone HRRR getting worse is the reason the current practical direction is
site-specific training.

## Inference Workflow

The intended operational chain is:

```text
new HRRR data -> WindNinja mass solver -> residual U-Net inference -> momentum-like rasters
```

Apply a checkpoint to a completed mass-solver run:

```bash
.venv/bin/python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/<run_name>/best.pt \
  --mass-run runtime/temp/<mass_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph \
  --output-speed-units mph
```

When a paired momentum run exists, include it for direct comparison:

```bash
.venv/bin/python -m ml.residual_unet.infer \
  --checkpoint ml/residual_unet/colab/results/<run_name>/best.pt \
  --mass-run runtime/temp/<mass_run> \
  --momentum-run runtime/temp/<momentum_run> \
  --out ml/residual_unet/outputs/inference/<run_name> \
  --source-root . \
  --speed-units mph
```

Current inference writes the trained `96 x 96` center crop, not a full-domain
raster. Outputs include corrected speed/direction rasters, corrected `u/v`,
predicted residuals, `metadata.json`, and comparison metrics when a momentum run
is supplied.

## Next Work

Priority next steps:

1. Rebuild the site-specific datasets with stricter day/event-level splits.
   Current HRRR splits are day-blocked, but the deterministic 10-day cycle can
   still be optimistic for weather events that span adjacent days.
2. Reserve some 7.5-degree midpoint controlled cases for validation/test so the
   midpoint matrix has an independent score. The builder now has this support;
   the remaining step is to rebuild/package the datasets.
3. Add more high-wind HRRR cases for Breck and Keystone if scorecard high-wind
   or direction-sector rows show weakness.
4. Test practical inference on fresh mass-solver runs and compare against a
   paired momentum solve before treating the model as operational.
5. Only after stricter splits, consider larger `base_channels` or architecture
   changes. Do not chase model complexity before the split/evaluation design is
   sound.

Potential code improvement: make the GCP data-build wrapper tolerate a small
number of known-bad HRRR days and continue packaging automatically, instead of
requiring a package-only repair when HRRR exits nonzero after mostly complete
output.

## Cleanup Boundary

Generated ML data is ignored by git. Safe cleanup targets only after GCS sync is
verified:

```text
ml/residual_unet/data/processed/
ml/residual_unet/outputs/
ml/residual_unet/colab/
runtime/ml/residual_unet/
```

Do not clean `runtime/ml/residual_unet/` or `runtime/temp/` while a GCP
data-generation run is active or before its GCS sync has been verified.

Preserve returned model artifacts unless intentionally discarding results:

```text
best.pt
latest.pt
train_log.csv
metrics.json
sample_metrics.csv
scorecard_report.md
scorecard_summary.json
comparison_report.md
```

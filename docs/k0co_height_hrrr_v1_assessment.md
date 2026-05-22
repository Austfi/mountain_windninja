# K0CO Height-Adjusted HRRR V1 Assessment

This note keeps the first K0CO height-adjusted HRRR test simple. The goal is to
decide whether a coarse terrain-aware HRRR forcing grid improves the final
WindNinja result at K0CO, not to build a complete mountain wind correction
model.

## Current Selected Method

For each HRRR analysis hour:

1. Read HRRR analysis `f00` from the public historical HRRR archive.
2. Use only 10 m U/V wind, 80 m U/V wind, and HRRR surface height.
3. Build a GMTED2010 500 m terrain grid for the WindNinja domain.
4. Compute:

```text
elevation_delta = GMTED 500 m elevation - HRRR surface HGT
height_weight = clamp(elevation_delta / 300 m, 0, 1)
```

5. Compute a simple coarse exposure gate from 3 km terrain-position index:

```text
exposure_weight = clamp(TPI_3km / 400 m, 0, 1)
weight = height_weight * exposure_weight
```

6. Blend the vector wind:

```text
u_adjusted = (1 - weight) * u10 + weight * u80
v_adjusted = (1 - weight) * v10 + weight * v80
```

7. Cap adjusted speed to 0.75x of the lower HRRR 10 m/80 m speed and 1.10x of
   the higher HRRR 10 m/80 m speed.
8. Feed the adjusted HRRR grid into WindNinja as gridded initialization.
9. WindNinja still runs on the high-resolution Berthoud terrain.

The current command setting is:

```bash
--adjustment-setting exposure-gate-400m-10-80-cap
```

## Output Layout

Canonical K0CO exposure-gated experiment outputs live under:

```text
runtime/validation/berthoud_pass_k0co_height_hrrr_exposure_gate_400m_10_80_cap/
```

Key files and folders:

| Path | Purpose | Cleanup rule |
|------|---------|--------------|
| `hrrr_comparison.html` | Full-period observed vs HRRR vs adjusted HRRR report | Keep |
| `hrrr_comparison_samples.csv` | Full-period K0CO HRRR-only sample table | Keep |
| `hrrr_comparison_metrics.csv` | Full-period K0CO HRRR-only metrics | Keep |
| `comparison_metrics.csv` | Full K0CO HRRR, adjusted HRRR, native WindNinja, and adjusted momentum WindNinja metrics | Keep |
| `comparison_metrics_mass.csv` | Full K0CO mass-solver comparison; useful as a failed-path diagnostic | Keep |
| `tuning/point_tuning_202601010000_202604010000.html` | Full-period HRRR-only tuning report | Keep |
| `tuning/point_tuning_202601010000_202604010000_metrics.csv` | Full-period tuning metrics | Keep |
| `tuning/point_fields_202601010000_202604010000.csv` | Cached K0CO point HRRR 10 m/80 m fields for tuning | Keep |
| `chunks/*/height_adjusted_hrrr_exposure_gate_400m_10_80_cap/` | Completed adjusted-HRRR momentum validation chunks | Keep |
| `chunks/*/height_adjusted_hrrr_exposure_gate_400m_10_80_cap_mass/` | Completed adjusted-HRRR mass validation chunks | Keep as diagnostic |
| `forcing/YYYYMMDDHHMM/` | Reusable adjusted-HRRR forcing grids | Keep while testing this method |
| `gmted_500m/` | Coarse DEM grid used for the adjustment | Keep |
| `cabtp_hrrr_adjusted/` | CABTP side-check data, not a tuning target | Keep as context |
| `debug_hrrr_cells/` | Earlier diagnostic for HRRR cell/elevation checks | Optional after final writeup |

Do not delete `runtime/temp/`, `chunks/`, `forcing/`, `static_data/berthoud_pass.*`,
or active `static_data/NINJAFOAM_*` directories while a WindNinja run is still
processing. It is safe to remove stale interim reports and one-off pilot chunks
after their final full-period equivalents exist.

## Assumptions To State Clearly

- HRRR 10 m wind is 10 m above HRRR's smoothed model terrain.
- K0CO sits on exposed high terrain where HRRR's smoothed terrain can be too low
  or too muted.
- HRRR 80 m wind is a reasonable upper-column sample to blend toward when the
  real terrain is higher than HRRR terrain.
- GMTED2010 at 500 m is enough for the adjustment field because this step is
  correcting the HRRR-scale forcing, not replacing WindNinja's high-resolution
  terrain downscaling.
- The method includes only one exposure signal: coarse 3 km TPI from the 500 m
  GMTED grid. Slope, curvature, forest/roughness, stability, and flow separation
  are intentionally deferred.
- The 300 m height blend scale, 400 m full-exposure TPI scale, and 10 m/80 m
  envelope cap are tuned for this first K0CO experiment, not proven universal
  values.

## Primary Comparison

Use completed K0CO samples only, deduplicated by station-hour.

| Result | Purpose |
|--------|---------|
| Observed K0CO | Truth data |
| HRRR | Raw parent model at the K0CO comparison point |
| Adjusted HRRR | Height-adjusted parent forcing before WindNinja |
| WindNinja from HRRR | Existing baseline WindNinja output |
| WindNinja from adjusted HRRR | New V1 output |

The most important final comparison is:

```text
WindNinja from HRRR vs WindNinja from adjusted HRRR
```

That isolates whether the adjusted gridded initialization improves the same
WindNinja path.

## Completed K0CO Jan-Apr Results

Full period: 2026-01-01 00:00 UTC through 2026-04-01 00:00 UTC.

| Result | Samples | Speed MAE | Bias | Direction MAE | Vector RMSE |
|--------|---------|-----------|------|---------------|-------------|
| HRRR | 2063 | 8.66 mph | -8.16 mph | 18.61 deg | 12.23 mph |
| Adjusted HRRR | 2063 | 4.66 mph | -0.56 mph | 18.76 deg | 9.95 mph |
| WindNinja from HRRR | 2063 | 8.89 mph | -7.80 mph | 17.77 deg | 12.76 mph |
| Momentum WindNinja from adjusted HRRR | 2063 | 6.61 mph | -3.09 mph | 15.78 deg | 10.66 mph |
| Mass WindNinja from adjusted HRRR | 2063 | 15.29 mph | +14.82 mph | 19.30 deg | 20.37 mph |

Conclusion for K0CO: the adjusted HRRR forcing itself is the clearest win, and
momentum WindNinja from adjusted HRRR improves over native WindNinja from HRRR.
The mass solver from adjusted HRRR overspeeds badly and should not be the
recommended K0CO path.

## CABTP Side Check

CABTP is close to K0CO but is a different terrain/exposure problem. Existing
adjusted-run rasters were sampled at CABTP without launching a new WindNinja run.

| Result | Samples | Speed MAE | Bias | Direction MAE | Vector RMSE |
|--------|---------|-----------|------|---------------|-------------|
| HRRR | 1925 | 4.75 mph | +3.47 mph | 24.35 deg | 8.86 mph |
| Adjusted HRRR exposure gate | 1925 | 4.71 mph | +3.42 mph | 24.41 deg | 8.82 mph |
| Momentum WN from adjusted HRRR | 1925 | 6.53 mph | -5.68 mph | 51.96 deg | 12.20 mph |
| Mass WN from adjusted HRRR | 1925 | 6.95 mph | +6.59 mph | 21.93 deg | 10.40 mph |

The exposure gate mostly leaves CABTP unchanged because the coarse 3 km TPI
signal is weak there:

```text
K0CO  TPI ~303 m, exposure weight ~0.757
CABTP TPI ~35 m,  exposure weight ~0.088
```

Do not treat CABTP as a validation win for the K0CO-tuned WindNinja path.

## Success Criteria

Primary:

- WindNinja from adjusted HRRR improves K0CO speed MAE versus WindNinja from HRRR.

Secondary:

- Vector RMSE improves or stays very close.
- Direction MAE does not materially worsen.
- Speed bias moves closer to zero without flipping into a large high bias.
- The high cap is not hit constantly. If many adjusted cells hit 1.35x, the
  adjustment is probably too aggressive.
- Improvement is not concentrated in one short period while degrading most other
  days.

## Required Final Diagnostics

For future candidate runs, produce one final summary table for the full
2026-01-01 00:00 UTC through 2026-04-01 00:00 UTC window:

- sample count
- speed MAE
- speed bias
- direction MAE
- vector RMSE

Then add these diagnostics:

- Metrics by month.
- Metrics by observed speed bin: `0-10`, `10-20`, `20-30`, `30+ mph`.
- Metrics by observed direction quadrant.
- Metrics by HRRR raw speed bin.
- Worst 20 adjusted-WindNinja vector-error hours.
- Best 20 adjusted-WindNinja improvement hours.
- Cap-hit frequency from adjustment metadata:
  - high cap cell fraction
  - low cap cell fraction
  - mean adjustment weight
- Bias check by period to make sure the adjustment did not only improve one
  storm pattern.

## Efficient Tuning Path

Do not rerun WindNinja for every possible tuning test first. Tune in two stages.

### Stage 1: HRRR-Only Tuning

This is cheap compared with WindNinja. Rebuild adjusted HRRR comparisons for the
full K0CO observed period and compare observed K0CO vs HRRR vs adjusted HRRR.

Tune only a small grid:

| Parameter | V1 | Trial Values |
|-----------|----|--------------|
| Elevation blend scale | `300 m` | `200`, `300`, `450`, `600 m` |
| Cap mode | `0.75x-1.35x` of HRRR 10 m speed | no cap, cap against HRRR 10 m speed, cap against both 10 m and 80 m speeds, HRRR 10 m cap with `2 mph` slack |
| High speed cap | `1.35x` | `1.20`, `1.35`, `1.50x`; `1.10x` when capped against both 10 m and 80 m speeds |
| Low speed cap | `0.75x` | `0.75`, `0.85x` |
| Raw baselines | HRRR 10 m | HRRR 10 m and HRRR 80 m |

Keep the metric target simple:

1. Speed MAE improves.
2. Bias moves toward zero without flipping too high.
3. Direction MAE is not materially worse.
4. High cap rate is not excessive.

Pick at most two candidate settings for WindNinja testing.

### Stage 2: WindNinja Confirmation

Run adjusted WindNinja only for:

1. The current V1 setting.
2. The best HRRR-only candidate: `balanced-300m-10-80-cap`.
3. One conservative candidate if the best setting looks aggressive.

Start each candidate with one week before spending the full seasonal runtime.

```bash
./deploy/gcp/mwn.sh validate-k0co-height-hrrr \
  --start 202601010000 \
  --end 202601080000 \
  --chunk-hours 24 \
  --adjustment-setting balanced-300m-10-80-cap \
  --skip-native
```

The balanced candidate writes to:

```text
runtime/validation/berthoud_pass_k0co_height_hrrr_balanced_300m_10_80_cap/
```

Only run the full Jan-Apr WindNinja comparison after the one-week candidate
improves the same core metrics.

## What Not To Add In V1

Keep these out of the first version unless the completed diagnostics clearly
show the simple height adjustment is insufficient:

- pressure-level interpolation
- station-trained correction
- Kalman/MOS correction
- slope/curvature terrain correction
- CNN or machine-learning downscaling
- roughness/forest correction
- station-specific hand tuning

Those may be valid later. V1 should answer one clean question first: does a
coarse height-aware HRRR forcing field improve K0CO WindNinja validation?

## Decision After Full Run

Use this decision table:

| Outcome | Next Step |
|---------|-----------|
| Speed MAE and vector RMSE improve, with sane bias and cap rates | Accept V1 and document it as K0CO-supported |
| Speed improves but vector worsens materially | Diagnose by wind direction; weaken blend |
| Bias flips strongly high | Reduce high cap or increase blend scale |
| High cap is hit often | Increase blend scale or lower high cap |
| Improvement appears only in one short event | Keep experimental and run more periods before accepting |
| Adjusted HRRR improves but adjusted WindNinja does not | Diagnose WindNinja response before more HRRR tuning |

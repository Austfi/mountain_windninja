# K0CO Height-Adjusted HRRR V1 Assessment

This note keeps the first K0CO height-adjusted HRRR test simple. The goal is to
decide whether a coarse terrain-aware HRRR forcing grid improves the final
WindNinja result at K0CO, not to build a complete mountain wind correction
model.

## Current V1 Method

For each HRRR analysis hour:

1. Read HRRR analysis `f00` from the public historical HRRR archive.
2. Use only 10 m U/V wind, 80 m U/V wind, and HRRR surface height.
3. Build a GMTED2010 500 m terrain grid for the WindNinja domain.
4. Compute:

```text
elevation_delta = GMTED 500 m elevation - HRRR surface HGT
weight = clamp(elevation_delta / 300 m, 0, 1)
```

5. Blend the vector wind:

```text
u_adjusted = (1 - weight) * u10 + weight * u80
v_adjusted = (1 - weight) * v10 + weight * v80
```

6. Cap adjusted speed to 0.75x through 1.35x of raw HRRR 10 m speed.
7. Feed the adjusted HRRR grid into WindNinja as gridded initialization.
8. WindNinja still runs on the high-resolution Berthoud terrain.

## Output Layout

Canonical K0CO experiment outputs live under:

```text
runtime/validation/berthoud_pass_k0co_height_hrrr/
```

Key files and folders:

| Path | Purpose | Cleanup rule |
|------|---------|--------------|
| `hrrr_comparison.html` | Full-period observed vs HRRR vs adjusted HRRR report | Keep |
| `hrrr_comparison_samples.csv` | Full-period K0CO HRRR-only sample table | Keep |
| `hrrr_comparison_metrics.csv` | Full-period K0CO HRRR-only metrics | Keep |
| `tuning/point_tuning_202601010000_202604010000.html` | Full-period HRRR-only tuning report | Keep |
| `tuning/point_tuning_202601010000_202604010000_metrics.csv` | Full-period tuning metrics | Keep |
| `tuning/point_fields_202601010000_202604010000.csv` | Cached K0CO point HRRR 10 m/80 m fields for tuning | Keep |
| `chunks/*/height_adjusted_hrrr/` | Completed adjusted-HRRR WindNinja validation chunks | Keep while run is active |
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
- Elevation difference is the only V1 terrain signal. Exposure, slope, curvature,
  forest/roughness, stability, and flow separation are intentionally deferred.
- The 300 m blend scale and 0.75x-1.35x cap are first-test constants, not proven
  universal values.

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

After the full run finishes, produce one final summary table for the full
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
2. The best HRRR-only candidate.
3. One conservative candidate if the best setting looks aggressive.

Start each candidate with one week before spending the full seasonal runtime.

```bash
./deploy/gcp/mwn.sh validate-k0co-height-hrrr \
  --start 202601010000 \
  --end 202601080000 \
  --chunk-hours 24 \
  --skip-native
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

# Validation Guide

Validation compares WindNinja output against Synoptic station observations. It is
an advanced workflow with an external auth dependency: the Synoptic token must
belong to an account with weather-data access.

## Recommended Path

For Berthoud Pass, use the study wrapper:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3
```

Then scale the same command to a longer window:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --end 202601080000 \
  --chunk-hours 24
```

The study wrapper:

- reads explicit stations from `config/stations/berthoud_pass_validation_manifest.csv`
- prepares Synoptic metadata and WindNinja point coordinates for those stations
- runs HRRR reanalysis without `--points-file`
- keeps per-chunk run directories under `runtime/temp/`
- compares WindNinja and parent HRRR rasters with `validate-rasters`
- writes aggregate outputs under `runtime/validation/berthoud_pass/`

Station selection is intentionally simple: edit the manifest CSV to choose one
station or a short list of stations, then rerun the same command. The study
workflow does not search for stations automatically.

## Berthoud Sampling Points

![Berthoud validation sampling points](assets/berthoud_validation_points.png)

For the current K0CO setup, raster validation samples the nearest WindNinja
100 m output cell, about 44 m from the AWOS location, and the nearest parent
HRRR 3 km cell, about 1.58 km from the AWOS location. Synoptic observations are
truth data only; HRRR still drives WindNinja.

## Current Berthoud Pilot

The 2026-01-01 00:00 UTC through 2026-01-04 00:00 UTC pilot produced 73 matched
K0CO station-hours. WindNinja remained biased low on speed, but improved the
main comparison metrics versus parent HRRR:

| Metric | WindNinja | HRRR |
|--------|-----------|------|
| Speed MAE | 7.20 mph | 8.85 mph |
| Speed RMSE | 8.57 mph | 9.82 mph |
| Direction MAE | 11.1 deg | 18.4 deg |
| Vector RMSE | 10.26 mph | 12.53 mph |

That is reasonable enough to continue with daily chunks for a month-scale run.

## Plotting Results

Generate static plots from completed chunk samples:

```bash
./deploy/gcp/mwn.sh plot-validation \
  --study-root runtime/validation/berthoud_pass \
  --title "Berthoud Pass Validation - January 2026"
```

The plot command reads `runtime/validation/berthoud_pass/chunks/*/samples.csv`,
deduplicates overlapping station-hours, and writes SVG plots plus an `index.html`
to `runtime/validation/berthoud_pass/plots/`. It can be rerun while a long
validation job is active; it only includes chunks that have already completed.

Outputs include:

- wind speed time series
- wind speed error time series
- direction absolute error time series
- observed-vs-modeled speed scatter
- daily error metrics
- `plot_summary.json` with the plotted sample count and headline metrics

For manual validation, use the lower-level flow:

1. Prepare station metadata with `synoptic-points`.
2. Run HRRR reanalysis without `--points-file`.
3. Keep the run directory with `--keep-temp`.
4. Compare WindNinja and parent HRRR rasters with `validate-rasters`.

```bash
./deploy/gcp/mwn.sh run --mode reanalysis \
  --start 202601010000 --end 202601010300 \
  --model HRRR \
  --domain berthoud_pass \
  --keep-temp --no-upload

./deploy/gcp/mwn.sh validate-rasters \
  --run-dir runtime/temp/berthoud_pass_20260101_0000_reanalysis_3h_HRRR \
  --metadata-file runtime/validation/berthoud_pass/station_metadata.json \
  --start 202601010000 --end 202601010300 \
  --samples-csv runtime/validation/berthoud_pass/manual_3h_samples.csv \
  --station-summary-csv runtime/validation/berthoud_pass/manual_3h_station_summary.csv \
  --group-summary-csv runtime/validation/berthoud_pass/manual_3h_group_summary.csv \
  --summary-json runtime/validation/berthoud_pass/manual_3h_summary.json
```

## Important Constraints

- WindNinja rejects `input_points_file` when `momentum_flag = true`.
- For momentum validation, do not pass `--points-file`; use `validate-rasters`
  after the run.
- Interrupted reanalysis windows do not resume. Rerun the chunk cleanly.
- Station selection is explicit. Add or remove rows in the station manifest.
- Start with a 3-hour smoke test, then a 24-hour pilot, before scaling to longer
  windows.
- `validate-study --plan` prints chunk/run paths without network calls or writes.

## More Detail

- [Command reference](commands.md#validate-rasters) - validation command syntax
- [Command reference](commands.md#validate-study) - chunked validation studies
- [Agent handoff](agent_handoff.md#synoptic-validation) - known auth and runtime caveats

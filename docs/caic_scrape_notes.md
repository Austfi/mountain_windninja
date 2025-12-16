# CAIC tabular scrape -> WindNinja point-init station CSVs

## Time handling (local -> UTC)
CAIC tabular.php pages present timestamps in America/Denver local time (with DST).
This pipeline parses local timestamps as tz-aware using zoneinfo and converts to UTC
for all internal processing. Output filenames reflect UTC hours and are aligned to the
top-of-hour UTC.

The CLI argument `--end-utc` is treated as the *inclusive* end hour of the window.
For `--hours 18`, outputs cover:
  start_utc = end_utc_aligned - 17 hours
  ...
  end_utc_aligned

If `--end-utc` contains minutes/seconds, it is floored to the top of that UTC hour.

## CAIC request windowing
We request a lookback window from CAIC using:
  tabular.php?st=CODE&date=YYYY-MM-DD+HH&range=RANGE_HOURS&unit=e&area=caic

`date` is computed from the UTC end hour converted into America/Denver local time.
`--range-hours` should be >= window length plus margin (default 48).

## Parsing robustness
Parsing is based on extracting the <pre> body and dynamically detecting the header
line starting with "Date Time". Columns are not hard-coded; whatever columns CAIC
provides are mapped by header position. This is meant to tolerate column additions or
re-ordering (e.g., SWIN appearing in different positions).

## Mapping to UTC hours and missing data
For each target UTC hour, we pick:
- an exact dt_utc match if present, else
- the nearest sample within +/- 30 minutes, else missing.

Missing station-hour values are written as blank cells in the WindNinja station CSV,
and missing hours are logged in manifest.json.

## Output format (WindNinja station file)
Each output file is a WindNinja point initialization “station file” CSV containing one
row per station (currently two stations). The header matches WindNinja’s documented
station file fields (Station_Name, Coord_Sys, Datum, Lat/YCoord, Lon/XCoord, Height,
Height_Units, Speed, Speed_Units, Direction, Temperature, Temperature_Units,
Cloud_Cover, Radius_of_Influence, Radius_of_Influence_Units).

We output GEOGCS/WGS84 and the station lat/lon in decimal degrees. Wind height is
10 meters AGL (as configured in the station registry).

## Extending the station list
Edit STATION_REGISTRY in scripts/build_keystone_point_series_inputs.py and add
additional station dicts with keys: code, name, lat, lon, height_m.

## Known fragile elements / limitations
- CAIC HTML structure changes (e.g., <pre> removed) would require parser updates.
- Ambiguous local times around DST “fall back” are inherently ambiguous without an
  explicit offset; zoneinfo uses Python’s default fold handling.
- WindNinja point initialization may not be supported with the momentum solver in some
  WindNinja versions; verify your WindNinja build before expecting point-init runs to work.

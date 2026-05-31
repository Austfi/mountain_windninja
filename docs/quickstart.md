# Quickstart

This path gets one real forecast running. Docker must be installed and running.
For Google Cloud VM startup, image setup, and shutdown commands, use
[GCP setup guide](gcp_setup.md) first; after you SSH into the VM, the commands
below are the same.

## 1. Clone And Init

```bash
git clone https://github.com/Austfi/mountain_windninja.git /opt/mountain_windninja
cd /opt/mountain_windninja
./deploy/gcp/mwn.sh init
```

`init` creates `runtime/`, `static_data/`, and `config/runtime.env` if missing,
then tries to pull `ghcr.io/austfi/mountain-windninja:3.12.2-herbie.3`. If image pull
fails, run:

```bash
./deploy/gcp/mwn.sh build-local
```

## 2. Download Terrain

Use one center point and a small square, roughly 10-12 km wide.

```bash
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain my_area \
  --label "My Area"
```

Coordinates are `latitude longitude`. For western US longitudes, values are
negative, for example `-106.08`.

This downloads both:

- `static_data/my_area.tif` DEM fallback
- `static_data/my_area.lcp` active LCP terrain

Advanced area inputs are also supported:

```bash
./deploy/gcp/mwn.sh fetch-terrain 39.65 -106.0 39.55 -106.15 --domain my_area
./deploy/gcp/mwn.sh fetch-terrain --area-file area.kml --padding-km 1 --domain my_area
```

## 3. Check, Smoke, Run

```bash
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh smoke
./deploy/gcp/mwn.sh run --hours 6
```

`smoke` uses domain-average wind and does not download NOAA weather data. If
`smoke` passes but `run --hours 6` fails, focus on weather-model download or
forecast settings.

## Output

Archives land in `runtime/archives/`. Use `--keep-temp` on `smoke` or `run` to
keep raw output in `runtime/temp/`.

## First Fixes

```bash
./deploy/gcp/mwn.sh clean
./deploy/gcp/mwn.sh smoke --keep-temp
```

If reanalysis fails with `Missing required GCS credentials`, rebuild the image:

```bash
./deploy/gcp/mwn.sh build-local
```

## Next Docs

- [Terrain guide](terrain.md)
- [Command reference](commands.md)
- [Validation guide](validation.md)
- [Scheduling guide](scheduling.md)

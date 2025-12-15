# mountain_windninja

Automated WindNinja HRRR wind forecasting system for mountain terrain.

## Overview

This system automatically runs [WindNinja](https://www.fs.usda.gov/rmrs/projects/windninja) hourly to generate high-resolution wind forecasts using NOAA HRRR data. Results are uploaded to Google Cloud Storage and made available via Google Earth network links.

## Features

- **Hourly automated runs** via cron at :15 past each hour
- **18-hour forecast window** using HRRR model data
- **30m resolution** terrain-following wind fields
- **10m AGL output** wind speed and direction
- **Google Earth KMZ output** with time-slider animation
- **Auto-updating network links** for live Google Earth visualization

## Architecture

```
scripts/
├── daily_run.py         # Main forecast execution script
├── hourly_run.py        # Cron entry point (runs forecasts)
├── run_cron.sh          # Wrapper script with OpenFOAM environment
├── create_time_series.py # KMZ bundling and time-series generation
├── gcs_manager.py       # Google Cloud Storage upload management
├── config_loader.py     # Configuration loading
└── utils.py             # Logging and utilities

config/
├── keystone_template.cfg # WindNinja configuration template
└── keystone_template_large.cfg # Large domain template
```

## Setup

1. Install WindNinja with OpenFOAM momentum solver
2. Configure GCS bucket and credentials
3. Set up cron job:
```bash
15 * * * * /path/to/scripts/run_cron.sh
```

## Output

- **Portal:** https://storage.googleapis.com/wrf-austin-bucket/index.html
- **Network Link:** Download `HRRR_Forecast.kml` for auto-updating Google Earth visualization
- **Direct Download:** `latest_forecast.kmz` contains bundled 18-hour forecast

## Data Sources

- **HRRR:** [NOAA High-Resolution Rapid Refresh](https://www.nco.ncep.noaa.gov/pmb/products/hrrr/)
- **Terrain:** USGS 30m DEM

## License

WindNinja is developed by the USDA Forest Service Rocky Mountain Research Station.

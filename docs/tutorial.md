# Tutorial: Running WindNinja from a Cloud Server

This tutorial walks through setting up and running WindNinja on a Google Cloud VM using the command line. It covers the same workflow as the desktop application -- loading elevation data, selecting an initialization method, configuring the solver, and viewing output -- but executed through terminal commands instead of the GUI.

For reference, the official WindNinja tutorials are available at [weather.firelab.org/windninja/tutorials](https://weather.firelab.org/windninja/tutorials/).

**Time estimate:** ~90 minutes (most of which is the one-time Docker build).

---

## Prerequisites

- A Google account with access to [Google Cloud Console](https://console.cloud.google.com)
- The latitude/longitude bounding box for your area of interest (see [Step 6](#step-6-determine-your-area-of-interest))

New GCP accounts receive $300 in free credits for 90 days. The VM used in this tutorial costs approximately $0.13/hour.

---

## Step 1: Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click the project dropdown at the top, then **New Project**
3. Name it (e.g., `windninja`) and click **Create**
4. Enable billing for the project when prompted

**Verify:** The project name appears in the top bar of the Console.

---

## Step 2: Create a VM

1. Navigate to **Compute Engine > VM instances** (the API may take a moment to enable on first use)
2. Click **Create Instance**
3. Configure with the following settings:

| Setting | Value |
|---------|-------|
| Name | `windninja` |
| Region | `us-central1 (Iowa)` |
| Zone | `us-central1-a` |
| Machine series | E2 |
| Machine type | `e2-standard-4` (4 vCPU, 16 GB RAM) |
| Boot disk OS | Ubuntu 22.04 LTS |
| Boot disk size | 50 GB |
| Boot disk type | Balanced persistent disk |

4. Click **Create**

The `e2-standard-4` provides 4 CPU cores and 16 GB of RAM. This is sufficient for domains up to approximately 30x30 km with the momentum solver. For larger domains or faster runs, `c2-standard-8` (8 cores, 32 GB) is recommended.

**Verify:** The VM appears in the instances list with a green status icon.

---

## Step 3: Connect to the VM

From the VM instances page, click **SSH** next to your VM. This opens a browser-based terminal connected to the remote machine.

Alternatively, if you have the `gcloud` CLI installed locally:

```bash
gcloud compute ssh windninja --zone=us-central1-a
```

**Verify:** The terminal prompt shows `username@windninja:~$`

---

## Step 4: Clone and Bootstrap

```bash
sudo git clone https://github.com/Austfi/mountain_windninja.git /opt/mountain_windninja
sudo chown -R $USER:$USER /opt/mountain_windninja
cd /opt/mountain_windninja
./deploy/gcp/bootstrap_repo.sh
```

The bootstrap script installs Docker and creates the initial directory structure. After it completes, log out and reconnect for Docker group permissions to take effect:

```bash
exit
```

Reconnect via SSH, then verify Docker is available:

```bash
cd /opt/mountain_windninja
docker ps
```

**Verify:** `docker ps` prints a table header with no error.

---

## Step 5: Build the Docker Image

```bash
./deploy/gcp/mwn.sh build
```

This compiles WindNinja (v3.12.2), OpenFOAM 9, GDAL 3.4.1, PROJ 8.2.1, and all dependencies from source inside a Docker container. The build takes approximately 30 minutes on `e2-standard-4`. Subsequent builds use Docker's layer cache and complete in seconds unless the Dockerfile changes.

If the build fails with a disk space error, resize the boot disk in the GCP Console (Compute Engine > Disks > Edit), then expand the filesystem:

```bash
sudo growpart /dev/sda 1
sudo resize2fs /dev/sda1
```

**Verify:** The build completes without errors.

---

## Step 6: Determine Your Area of Interest

WindNinja requires a bounding box defined by four coordinates:

| Parameter | Description |
|-----------|-------------|
| North | Latitude of the northern edge |
| East | Longitude of the eastern edge |
| South | Latitude of the southern edge |
| West | Longitude of the western edge |

To find coordinates: open [Google Maps](https://www.google.com/maps), navigate to your area, and right-click to copy the latitude/longitude of the northwest and southeast corners.

Guidelines:
- Western hemisphere longitudes are negative (e.g., `-106.0`)
- For initial testing, keep the domain under 15x15 km
- Domains larger than 50x50 km will require significant computation time and memory

---

## Step 7: Download Elevation Data

This is equivalent to loading an elevation file in the desktop application's **Input** tab. WindNinja accepts DEM files (`.tif`, `.asc`) and LCP landscape files (`.lcp`).

### Option A: USGS 3DEP (US only, no API key required)

Downloads lidar-derived elevation data at 10m or 30m resolution. Available for the continental United States.

```bash
./deploy/gcp/mwn.sh fetch-dem <north> <east> <south> <west> static_data/my_area.tif us 10
```

Example for Keystone, CO:

```bash
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif us 10
```

### Option B: SRTM via OpenTopography (global, 30m)

Requires a free API key from [opentopography.org](https://opentopography.org/) (create an account, then go to Dashboard > Request API Key).

Add the key to the configuration:

```bash
nano config/runtime.env
```

Set `CUSTOM_SRTM_API_KEY=your_key_here`, save with **Ctrl+O**, exit with **Ctrl+X**.

```bash
./deploy/gcp/mwn.sh fetch-dem 39.65 -106.0 39.55 -106.15 static_data/my_area.tif world 30
```

### Option C: LANDFIRE LCP (US only, includes vegetation data)

LCP files contain elevation plus seven bands of vegetation and fuel data (canopy cover, canopy height, fuel model, etc.). When an LCP is used, WindNinja reads vegetation directly from the file rather than applying a uniform assumption. This corresponds to loading a `.lcp` file in the desktop application.

```bash
./deploy/gcp/mwn.sh fetch-lcp 39.65 -106.0 39.55 -106.15 static_data/my_area.lcp
```

LANDFIRE generates the file server-side; downloads may take 5-15 minutes.

### DEM vs LCP

| | DEM (.tif) | LCP (.lcp) |
|---|---|---|
| Data | Elevation only | Elevation + 7 vegetation/fuel bands |
| Vegetation handling | Uniform (grass, brush, or trees) | Per-cell vegetation from LANDFIRE |
| Availability | Global (3DEP, SRTM, GMTED) | US only (LANDFIRE) |
| Recommended use | Initial testing, non-US areas | Fire weather, forested terrain, production |

**Verify:** `ls -la static_data/` shows your terrain file with a non-zero file size.

---

## Step 8: Configure the Domain

A domain maps a name to an elevation file and a configuration template. This is the equivalent of the desktop application's file selection and input panel.

Edit `config/domains.json`:

```bash
nano config/domains.json
```

Set the `elevation_file` to match the filename you downloaded:

```json
{
  "default_domain": "my_area",
  "domains": {
    "my_area": {
      "label": "My Area",
      "template": "config/template.cfg",
      "elevation_file": "my_area.tif"
    }
  }
}
```

If you downloaded an LCP, use `"my_area.lcp"` instead.

Confirm the domain ID in `config/runtime.env` matches:

```bash
grep MWN_DOMAIN_ID config/runtime.env
```

It should show `MWN_DOMAIN_ID=my_area`. If not, edit the file to match.

**Verify:** `./deploy/gcp/mwn.sh check` passes with no errors.

---

## Step 9: Understanding the Configuration Template

The file `config/template.cfg` controls WindNinja's behavior. It maps directly to the settings available in the desktop application's **Input**, **Solver**, and **Output** tabs.

### Input Settings

| Config Option | Desktop Equivalent | Default |
|---|---|---|
| `initialization_method` | Initialization method dropdown | `wxModelInitialization` |
| `wx_model_type` | Weather model selector | `NOMADS-HRRR-CONUS-3-KM` |
| `forecast_duration` | Forecast duration | Set at runtime via `--hours` |

### Solver Settings

| Config Option | Desktop Equivalent | Default |
|---|---|---|
| `momentum_flag` | Solver type (Mass vs Momentum) | `true` |
| `mesh_resolution` | Mesh resolution slider | `100.0` m |
| `number_of_iterations` | Number of iterations | `300` |
| `num_threads` | Number of processors | `4` |
| `diurnal_winds` | Diurnal winds checkbox | `true` |

Setting `momentum_flag = false` uses the conservation of mass solver only (equivalent to selecting "Mass Conservation" in the desktop). This runs in seconds rather than minutes but produces less accurate results in complex terrain.

### Output Settings

| Config Option | Desktop Equivalent | Default |
|---|---|---|
| `output_wind_height` | Output height | `10.0` m |
| `output_speed_units` | Speed units | `mph` |
| `write_goog_output` | Google Earth (KMZ) checkbox | `true` |
| `write_ascii_output` | ASCII grids checkbox | `true` |
| `write_shapefile_output` | Shapefile checkbox | `false` |
| `write_pdf_output` | PDF checkbox | `false` |

To modify these settings:

```bash
nano config/template.cfg
```

Changes apply to all subsequent runs.

---

## Step 10: Run a Simulation

### Weather Model Initialization (Forecast)

This is the equivalent of the desktop's **Tutorial 4: Weather Model Initialization**. WindNinja downloads the latest weather forecast from NOAA and downscales it to your terrain.

```bash
./deploy/gcp/mwn.sh run --hours 3
```

This runs a 3-hour forecast using the default HRRR model. During execution, WindNinja:

1. Downloads the latest HRRR forecast from NOAA's NOMADS server
2. Interpolates the coarse (3 km) model data to your terrain grid
3. Runs the solver for each forecast hour
4. Writes output files (KMZ, ASCII grids) for each hour

Expected runtime: 5-15 minutes for a 3-hour forecast on a ~10 km domain with the momentum solver.

To use a different weather model:

```bash
./deploy/gcp/mwn.sh run --hours 6 --model GFS
```

Available models:

| Flag | WindNinja Model Name | Resolution | Coverage | Max Hours |
|------|---------------------|-----------|----------|-----------|
| `HRRR` | `NOMADS-HRRR-CONUS-3-KM` | 3 km | CONUS | 18 |
| `NBM` | `NOMADS-NBM-CONUS-2.5-KM` | 2.5 km | CONUS | 264 |
| `NAM` | `NOMADS-NAM-NEST-CONUS-3-KM` | 3 km | CONUS | 60 |
| `RAP` | `NOMADS-RAP-CONUS-13-KM` | 13 km | CONUS | 21 |
| `GFS` | `NOMADS-GFS-GLOBAL-0.25-DEG` | ~25 km | Global | 384 |

### Domain Average Initialization

This is the equivalent of the desktop's **Tutorial 1: The Basics**. You specify a single wind speed and direction; WindNinja applies it uniformly across the domain and adjusts for terrain effects.

```bash
./deploy/gcp/mwn.sh run --mode domain-average --speed 15 --direction 270
```

Wind direction is specified in degrees (0 = North, 90 = East, 180 = South, 270 = West). This mode does not require an internet connection.

Additional options:

```bash
./deploy/gcp/mwn.sh run --mode domain-average --speed 10 --direction 180 --speed-units mps
```

### Historical Reanalysis

Uses archived HRRR data from the Google Cloud HRRR archive (available from 2014 to present):

```bash
./deploy/gcp/mwn.sh run --mode reanalysis --hours 12
```

Only the HRRR model is available for reanalysis.

**Verify:** The run completes without errors and reports archiving output files.

---

## Step 11: Retrieve and View Output

Output is archived in `runtime/archives/`. To list available output:

```bash
ls runtime/archives/
```

### Download to Your Local Machine

From a terminal on your local machine (not the SSH session):

```bash
gcloud compute scp windninja:/opt/mountain_windninja/runtime/archives/*.zip ~/Downloads/ --zone=us-central1-a
```

### View in Google Earth

1. Unzip the downloaded archive
2. Open the `.kmz` files in [Google Earth](https://earth.google.com/web/) or [Google Earth Pro](https://www.google.com/earth/about/versions/#earth-pro) (desktop)
3. Wind vectors display as arrows overlaid on the terrain. Arrow direction indicates wind direction; color indicates speed.

If the archive contains a `*_playable.kmz` file, it includes all forecast hours with a time slider for animation.

### Inspect Raw Output

To keep individual hourly files for inspection, use the `--keep-temp` flag:

```bash
./deploy/gcp/mwn.sh run --hours 3 --keep-temp
```

Raw output is written to `runtime/temp/<run_directory>/` and includes:

| File Pattern | Description |
|---|---|
| `*_vel.asc` | Wind speed ASCII grid (one per hour) |
| `*_ang.asc` | Wind direction ASCII grid (one per hour) |
| `*.kmz` | Individual hourly KMZ files |
| `*.cfg` | The generated WindNinja configuration file |

ASCII grids can be opened in GIS software (QGIS, ArcGIS) for further analysis.

---

## Step 12: Stop the VM

A running VM incurs charges. A stopped VM retains all data on disk but costs only ~$5/month for storage. Always stop the VM when not in use.

From the GCP Console: **Compute Engine > VM instances** > select the VM > **Stop**.

Or from your local terminal:

```bash
gcloud compute instances stop windninja --zone=us-central1-a
```

To resume later:

```bash
gcloud compute instances start windninja --zone=us-central1-a
gcloud compute ssh windninja --zone=us-central1-a
cd /opt/mountain_windninja
```

The Docker image, terrain files, and all output persist across stop/start cycles.

---

## Desktop-to-CLI Reference

For users familiar with the WindNinja desktop application, this table maps GUI actions to their CLI equivalents:

| Desktop Action | CLI Equivalent |
|---|---|
| File > Open Elevation File | `mwn.sh fetch-dem` or place file in `static_data/` |
| Input > Domain Average | `mwn.sh run --mode domain-average --speed N --direction N` |
| Input > Weather Model > HRRR | `mwn.sh run --model HRRR --hours N` |
| Input > Weather Model > GFS | `mwn.sh run --model GFS --hours N` |
| Input > Vegetation | `MWN_SURFACE_VEGETATION` in `config/runtime.env` |
| Solver > Mass Conservation | `momentum_flag = false` in `config/template.cfg` |
| Solver > Momentum | `momentum_flag = true` in `config/template.cfg` |
| Solver > Mesh Resolution | `mesh_resolution` in `config/template.cfg` |
| Solver > Iterations | `number_of_iterations` in `config/template.cfg` |
| Output > Google Earth | `write_goog_output = true` in `config/template.cfg` |
| Output > Shapefiles | `write_shapefile_output = true` in `config/template.cfg` |
| Output > PDF | `write_pdf_output = true` in `config/template.cfg` |
| Output > Speed Units | `output_speed_units` in `config/template.cfg` |
| Solve | `mwn.sh run` |

---

## Troubleshooting

**Weather data download fails:** NOAA's NOMADS servers experience periodic outages. Wait a few minutes and retry. Alternatively, try a different model (`--model GFS` uses a separate server).

**"Error reading elevation file":** The terrain file may have a geographic coordinate system (lat/lon) instead of a projected one (UTM). Check with `./deploy/gcp/mwn.sh shell` then `gdalinfo static_data/your_file.tif`. If the coordinate system shows `GEOGCS`, reproject:

```bash
gdalwarp -t_srs EPSG:32613 static_data/input.tif static_data/output.tif
```

Replace `32613` with the appropriate UTM zone for your area.

**Momentum solver errors ("blockMesh", "posix_spawnp"):** These indicate OpenFOAM environment issues. When running manually inside the container (`mwn.sh shell`), source the environment first:

```bash
source /opt/openfoam9/etc/bashrc
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
```

**Slow runs:** Reduce computation time by increasing `mesh_resolution` (larger value = coarser grid), reducing `--hours`, or setting `momentum_flag = false` for the mass-only solver.

---

## Next Steps

- **[Command Reference](commands.md)** -- all available flags, models, and run options
- **[GCP Setup Guide](gcp_setup.md)** -- cost management, budget alerts, auto-scheduling, Spot VMs
- **[WindNinja Reference](windninja_reference.md)** -- full configuration file reference, environment variables, solver details
- **[Official WindNinja Tutorials](https://weather.firelab.org/windninja/tutorials/)** -- detailed tutorials from the WindNinja development team
- **[WindNinja Wiki](https://github.com/firelab/windninja/wiki)** -- advanced configuration, weather models, developer documentation

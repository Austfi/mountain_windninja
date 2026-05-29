# GCP Setup Guide

Step-by-step instructions to get WindNinja running on a Google Cloud VM from scratch. No prior GCP experience required.

## What You Need Before Starting

- A Google account
- A credit card for GCP billing (new accounts get $300 free credit)
- Know one center point for the area you want to simulate

You do **not** need to pre-download terrain files. This guide shows how to download them directly on the VM.

## Fast Path: Existing VM

Use this when the VM already exists and you just need to start it, update the
repo, and run forecasts or historical reanalysis.

From your local machine:

```bash
gcloud compute instances start windninja --zone us-central1-a
gcloud compute ssh windninja --zone us-central1-a
```

Replace `us-central1-a` with the VM's actual zone.

On the VM:

```bash
cd /opt/mountain_windninja
git pull --ff-only

# Make sure Docker is reachable from this SSH session.
docker ps

# Create missing local dirs/config and use the published GHCR image.
./deploy/gcp/mwn.sh init --image pull
./deploy/gcp/mwn.sh pull ghcr.io/austfi/mountain-windninja:3.12.2-herbie.2

# Check the active domain before spending time on a real run.
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh smoke
```

If `docker ps` prints a permission error, exit and SSH back in. Docker group
membership is applied when the login session starts.

Run a forecast:

```bash
./deploy/gcp/mwn.sh run --model HRRR --hours 6
./deploy/gcp/mwn.sh run --model NBM --hours 12
```

Run historical HRRR reanalysis:

```bash
./deploy/gcp/mwn.sh run --mode reanalysis \
  --start 202601010000 \
  --end 202601020000 \
  --model HRRR \
  --keep-temp \
  --no-upload
```

For long reanalysis or validation jobs, start them inside `tmux` so the run
continues if your SSH window disconnects:

```bash
# If tmux is missing on a fresh VM:
sudo apt-get install -y tmux

tmux new -s mwn

# Run the long command inside tmux.
MWN_NUM_THREADS=6 ./deploy/gcp/mwn.sh validate-study berthoud_pass_k0co \
  --start 202601010000 \
  --end 202604010000 \
  --chunk-hours 24

# Detach with Ctrl-b, then d. Reattach later:
tmux attach -t mwn
```

Useful checks from a second SSH session:

```bash
docker ps
docker stats
df -h .
find runtime/validation/berthoud_pass_k0co/chunks -maxdepth 2 -name summary.json | wc -l
```

When finished, exit the SSH session and stop the VM from your local machine:

```bash
exit
gcloud compute instances stop windninja --zone us-central1-a
```

Stopping the VM stops compute billing. Disk storage remains until the VM or disk
is deleted.

## Step 1: Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click the project dropdown at the top, then "New Project"
3. Name it something like `windninja`
4. Enable billing for the project

## Understanding GCP Costs Before You Start

GCP bills by the second while your VM is running. **You are not charged when the VM is stopped.** Your disk storage still costs money when stopped (~$0.10/month per 10 GB), but that's negligible. The single most important cost-saving habit is: **stop your VM when you're not using it.**

New accounts get **$300 in free credits** for 90 days. This is enough to run WindNinja for hundreds of hours. You won't be charged real money unless you manually upgrade to a paid account after the trial.

### Which VM to Choose

WindNinja is CPU-intensive (especially with the momentum/OpenFOAM solver). Here are the best options, all priced for the `us-central1` (Iowa) region:


| Machine Type    | vCPUs | RAM   | Cost/Hour (On-Demand) | Cost/Hour (Spot) | Best For                                  |
| --------------- | ----- | ----- | --------------------- | ---------------- | ----------------------------------------- |
| `e2-standard-4` | 4     | 16 GB | **$0.134**            | $0.065           | Starting out, small domains, tight budget |
| `e2-standard-8` | 8     | 32 GB | $0.268                | $0.130           | Medium domains, faster runs               |
| `c2-standard-4` | 4     | 16 GB | $0.209                | $0.068           | Better CPU performance per core           |
| `c2-standard-8` | 8     | 32 GB | $0.418                | $0.136           | Large domains, production runs            |


**Recommendation for beginners: `e2-standard-4`** -- cheapest option that works well. At $0.134/hour, running it 4 hours a day for a month costs about $16. With the $300 free credit, you can run it for ~2,200 hours before paying anything.

**If you need faster runs:** `c2-standard-8` has dedicated high-frequency Intel CPUs (not shared) and 8 cores. It's 3x the price but runs WindNinja significantly faster on larger domains.

The current test VM, `mwj-test`, uses `e2-highcpu-8` in `us-central1-b`
with Ubuntu 22.04. That shape has 8 vCPUs but only 8 GB RAM, which is adequate
for the small Berthoud validation domain. For larger terrain domains, prefer a
standard or C2 machine with more memory.

### Spot VMs: 50-70% Cheaper (Advanced)

Spot VMs use Google's spare capacity at a huge discount. The catch: Google can shut them down with 30 seconds notice if they need the capacity back. This rarely happens, but it means:

- **Good for:** Manual one-off runs where you can just re-run if interrupted
- **Bad for:** Scheduled automatic forecasts that need to complete reliably

To create a Spot VM, check **"Spot"** under "VM provisioning model" when creating the instance. Everything else works the same.

### Monthly Cost Estimates

These assume you **stop the VM when not in use** (which you should always do):


| Usage Pattern                     | Machine Type         | Monthly Cost |
| --------------------------------- | -------------------- | ------------ |
| Occasional (2 hours/week)         | e2-standard-4        | ~$1.07       |
| Regular manual runs (2 hours/day) | e2-standard-4        | ~$8.04       |
| Heavy use (8 hours/day)           | e2-standard-4        | ~$32.16      |
| 24/7 automated scheduler          | e2-standard-4        | ~$97.84      |
| Occasional with Spot              | e2-standard-4 (Spot) | ~$0.52       |
| Heavy use with C2                 | c2-standard-8        | ~$100.32     |


All estimates plus ~$5/month for 50 GB disk storage.

### Region Selection

Pick a region close to you for lower latency when SSH-ing in. The cheapest US regions are:


| Region        | Location       | Notes                                    |
| ------------- | -------------- | ---------------------------------------- |
| `us-central1` | Iowa           | Cheapest, most availability, recommended |
| `us-east1`    | South Carolina | Same price as Iowa                       |
| `us-west1`    | Oregon         | Same price, free-tier eligible           |
| `us-east4`    | Virginia       | ~10% more expensive                      |
| `us-west4`    | Las Vegas      | ~10% more expensive                      |


**Use `us-central1` unless you have a reason not to.** It has the most machine type availability and is the cheapest.

## Step 2: Create a VM

1. Go to **Compute Engine > VM instances** (it may take a minute to enable the API the first time)
2. Click **Create Instance**
3. Settings:
  - **Name:** `windninja`
  - **Region:** `us-central1 (Iowa)` -- cheapest and most available
  - **Zone:** Any (e.g., `us-central1-a`)
  - **Machine configuration:**
    - Series: **E2** (or C2 if you want faster runs)
    - Machine type: **e2-standard-4** (4 vCPU, 16 GB RAM)
  - **VM provisioning model:** **Standard** (or Spot for cheaper one-off runs)
  - **Boot disk:** Click **"Change"**
    - OS: **Ubuntu 22.04 LTS**
    - Size: **50 GB** (Docker image is large)
    - Type: **Balanced persistent disk** (cheapest SSD option)
  - **Firewall:** Leave both unchecked (you don't need web access)
  - **Networking > Network interfaces:** Leave defaults (you only need SSH)
4. Click **Create**

The VM will be ready in about 30 seconds. You'll see it listed with a green checkmark.

**Important:** The estimated monthly cost shown in the right panel assumes 24/7 running. You will pay much less because you'll stop the VM when not using it.

## Step 3: SSH Into the VM

From the VM instances page, click **SSH** next to your VM. This opens a browser terminal. Alternatively, install `gcloud` locally and run:

```bash
gcloud compute ssh windninja

# Current test VM:
gcloud compute ssh mwj-test --zone us-central1-b
```

## Step 4: Clone and Bootstrap

Fresh Ubuntu images sometimes have first-boot package work still running. If
`apt-get update` reports a lock, wait a minute and retry. Do not delete lock
files. Use this simple path first:

```bash
# Repair first-boot or interrupted apt state if needed.
sudo dpkg --configure -a
sudo apt-get update
sudo apt-get install -y git

sudo mkdir -p /opt
sudo chown "$USER:$USER" /opt

git clone https://github.com/Austfi/mountain_windninja.git /opt/mountain_windninja
cd /opt/mountain_windninja

./deploy/gcp/bootstrap_repo.sh
newgrp docker
docker run hello-world

./deploy/gcp/mwn.sh init --image pull
```

The bootstrap script installs Docker and Docker Compose from Ubuntu packages,
creates `runtime/`, `static_data/`, and `config/runtime.env` when needed, and
adds your user to the Docker group. If Docker works with `sudo` but not as your
user, run `newgrp docker` or log out and SSH back in.

## Step 5: Pull Or Build the Docker Image

Normal operators should use the published GitHub Container Registry image. It
already includes WindNinja, OpenFOAM, GDAL, Python dependencies, and the public
HRRR pastcast patch.

```bash
./deploy/gcp/mwn.sh pull
```

This records the image in `config/runtime.env` as `MWN_DOCKER_IMAGE`, normally:

```text
MWN_DOCKER_IMAGE=ghcr.io/austfi/mountain-windninja:3.12.2-herbie.2
```

If pulling fails, or if you changed `Dockerfile` or files under `docker/`, build
locally:

```bash
./deploy/gcp/mwn.sh build-local
```

Local build takes **~30 minutes** the first time. It compiles WindNinja,
OpenFOAM, GDAL, and dependencies from source inside the Docker image.

Changes under `scripts/`, `config/`, or `docs/` do not require a Docker rebuild
because those directories are bind-mounted into the container.

### Manual Docker Install Fallback

Use this only if `bootstrap_repo.sh` cannot install a working Docker/Compose
stack. The `docker-ce` package is not available on a brand-new Ubuntu image until
Docker's apt repository is added:

```bash
sudo apt-get install -y ca-certificates curl gnupg git nano tmux

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo docker run hello-world

sudo usermod -aG docker "$USER"
newgrp docker
docker run hello-world
docker compose version
```

## Step 6: Get Your Terrain Data

You need elevation data for the area you want to simulate. There are two main options:

### Option A: Download a DEM (simplest)

For US domains, the easiest path downloads both a DEM fallback and an LCP active
terrain file. The LCP includes vegetation/fuel bands; the DEM remains available
if LANDFIRE fails or for debugging.

**Method 1: Use the built-in downloader (recommended)**

Find a center point for your area using Google Maps (right-click any point to see lat/lon). Then:

```bash
# Download/register DEM + LCP terrain for the Keystone, CO area
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"

# Download DEM + LCP terrain for a larger area in Summit County
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.15 --size-km 30 \
  --domain summit \
  --label "Summit County"
```

The DEM defaults to USGS 3DEP (`us`) at 10 m and does not require an API key in
the United States. For global DEM-only SRTM, use `fetch-dem`, pass an output
path, `srtm`, and `30`, then add a free OpenTopography API key to
`config/runtime.env`:

```
CUSTOM_SRTM_API_KEY=your_key_here
```

**Method 2: Download from USGS National Map**

1. Go to [apps.nationalmap.gov/downloader](https://apps.nationalmap.gov/downloader/)
2. Zoom to your area of interest
3. Under "Data," check "Elevation Products (3DEP)"
4. Select "1/3 arc-second DEM" (10m resolution) or "1 arc-second DEM" (30m)
5. Click "Search Products," find your tile, and download the GeoTIFF
6. Upload to your VM:

```bash
# From your local machine
gcloud compute scp ~/Downloads/USGS_13_n40w106_20230601.tif \
  windninja:/opt/mountain_windninja/static_data/
```

**Method 3: Download from OpenTopography**

1. Go to [portal.opentopography.org](https://portal.opentopography.org/)
2. Use "Find Data" to select your area
3. Choose a dataset (SRTM GL1 at 30m is good for most uses, COP30 for global coverage)
4. Download the GeoTIFF
5. Upload to your VM as shown above

### Option B: Download an LCP (more accurate)

An LCP (Landscape) file includes elevation plus 7 bands of vegetation and fuel data. This gives WindNinja real vegetation data instead of a uniform assumption, producing more accurate results.

**Method 1: Use the built-in downloader (recommended)**

```bash
# Download LCP for the Keystone, CO area
./deploy/gcp/mwn.sh fetch-lcp --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"
```

This downloads from LANDFIRE (US Forest Service). It may take several minutes as the LANDFIRE server processes your request. Only available for the United States.

**Method 2: Download from LANDFIRE Map Viewer**

1. Go to [landfire.gov/viewer](https://www.landfire.gov/viewer/)
2. Use the download tool to draw a rectangle around your area
3. Click the product version toggle and select "Surface and Canopy"
4. Check the box for "Landscape" files
5. Enter your email and click Download
6. Upload the file to your VM:

```bash
gcloud compute scp ~/Downloads/landscape.tif \
  windninja:/opt/mountain_windninja/static_data/

# If it's a GeoTIFF landscape, convert to LCP:
./deploy/gcp/mwn.sh lcp-build static_data/landscape.tif static_data/my_area.lcp
```

**Method 3: Use the LANDFIRE Python package**

For automated/scripted downloads, the `landfire` Python package provides a clean API:

```python
import landfire
lf = landfire.Landfire(bbox="-106.15 39.55 -106.0 39.65")
lf.request_data(
    layers=["ELEV2020", "SLPD2020", "ASP2020", "240FBFM40",
            "240CC", "240CH", "240CBD", "240CBH"],
    output_path="./static_data/keystone_lf.zip"
)
```

Install with `pip install landfire`. See [landfire-python.firesci.io](https://landfire-python.firesci.io/) for full docs.

### DEM vs LCP: Which Should I Use?


|               | DEM                                       | LCP                                            |
| ------------- | ----------------------------------------- | ---------------------------------------------- |
| Data included | Elevation only                            | Elevation + 7 vegetation/fuel bands            |
| Accuracy      | Good (uses uniform vegetation assumption) | Best (uses real vegetation data)               |
| Availability  | Global (SRTM, 3DEP, COP30)                | US only (LANDFIRE)                             |
| Best for      | Quick tests, non-US areas, simple terrain | Production runs, fire modeling, forested areas |
| File size     | Smaller                                   | Larger                                         |


### Terrain Data Requirements

For all terrain files (DEM or LCP), WindNinja requires:

- **Projected coordinate system** (UTM is recommended). Geographic lat/lon will fail.
- **North-up orientation**
- **Elevation in meters**
- **No data gaps** (holes in the terrain)
- **Recommended area:** Under 50 x 50 km for reasonable run times on a 4-CPU VM

To check your file, open a shell and use `gdalinfo`:

```bash
./deploy/gcp/mwn.sh shell
gdalinfo static_data/your_file.tif
```

Look for "Coordinate System" (should show UTM or another projected CRS, not "GEOGCS") and "Band 1" (should show min/max elevation values that make sense).

## Step 7: Configure Your Domain

Edit `config/domains.json` to point to your terrain file:

```bash
nano config/domains.json
```

```json
{
  "default_domain": "my_area",
  "domains": {
    "my_area": {
      "label": "My Custom Area",
      "template": "config/template.cfg",
      "elevation_file": "my_terrain.tif"
    }
  }
}
```

Then update `config/runtime.env`:

```bash
nano config/runtime.env
```

```
MWN_DOMAIN_ID=my_area
```

If you have an LCP file instead of a DEM, use that as the `elevation_file`. WindNinja automatically detects the file type.

### Key Settings in runtime.env


| Setting                  | What it means                                                 | Default |
| ------------------------ | ------------------------------------------------------------- | ------- |
| `MWN_DOMAIN_ID`          | Which domain to run (must match a key in `domains.json`)      | `my_area` |
| `MWN_DOCKER_IMAGE`       | Docker image used by Compose                                  | `mountain-windninja:local` |
| `MWN_SURFACE_VEGETATION` | Default vegetation for DEM runs: `grass`, `brush`, or `trees` | `trees` |
| `MWN_GCS_UPLOAD_ENABLED` | Upload results to a GCS bucket                                | `false` |
| `MWN_GCS_BUCKET`         | Your GCS bucket name (if uploading)                           | empty   |
| `CUSTOM_SRTM_API_KEY`    | OpenTopography API key for `fetch-dem`                        | empty   |


## Step 8: Run Your First Simulation

```bash
# Verify everything is set up
./deploy/gcp/mwn.sh check

# Run a quick domain-average smoke test on your active domain
./deploy/gcp/mwn.sh smoke

# Run a quick 6-hour forecast
./deploy/gcp/mwn.sh run --hours 6

# If that works, run a full 18-hour HRRR forecast
./deploy/gcp/mwn.sh run --hours 18
```

### Forecast, Reanalysis, And Validation Runbook

Use `forecast` mode for current/future weather. Use `reanalysis` for historical
HRRR pastcast. Use `validate-study` when you need observed station comparison
against both parent HRRR and WindNinja-downscaled HRRR.

Forecast examples:

```bash
# Default HRRR forecast for the default domain
./deploy/gcp/mwn.sh run --hours 6

# NBM forecast, useful for calibrated blended guidance
./deploy/gcp/mwn.sh run --model NBM --hours 12

# Long-range global forecast
./deploy/gcp/mwn.sh run --model GFS --hours 48
```

Historical HRRR reanalysis examples:

```bash
# Last 12 hours ending at the current UTC hour
./deploy/gcp/mwn.sh run --mode reanalysis --hours 12 --model HRRR

# Exact UTC window; use this for reproducible event analysis
./deploy/gcp/mwn.sh run --mode reanalysis \
  --start 202601010000 \
  --end 202601020000 \
  --model HRRR \
  --domain keystone \
  --keep-temp \
  --no-upload
```

Chunked validation examples:

```bash
# Print the plan first
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3 \
  --plan

# Run a short pilot
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3

# Run a full month in 24-hour chunks
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --end 202602010000 \
  --chunk-hours 24

# Build plots after chunks finish
./deploy/gcp/mwn.sh plot-validation \
  --study-root runtime/validation/berthoud_pass \
  --title "Berthoud Pass Validation - January 2026"
```

Historical validation does not feed observations into WindNinja. HRRR drives the
simulation. Synoptic observations are used afterward for station coordinates,
sensor heights, and observed wind comparison.

### Long-Run Operating Notes

Use a terminal multiplexer for anything that takes more than a few minutes:

```bash
# If tmux is missing on a fresh VM:
sudo apt-get install -y tmux

tmux new -s mwn
./deploy/gcp/mwn.sh run --mode reanalysis --start 202601010000 --end 202601080000 --model HRRR
```

Detach with `Ctrl-b`, then `d`. Reattach with:

```bash
tmux attach -t mwn
```

Monitor health from a second SSH session:

| Command | What to check |
|---------|---------------|
| `docker ps` | Confirms the run container is still alive |
| `docker stats` | CPU should be active during OpenFOAM solver steps |
| `df -h .` | Keep enough disk free for weather caches, temp runs, and Docker |
| `./deploy/gcp/mwn.sh logs` | Scheduler logs, if running scheduled forecasts |

For chunked validation, count completed chunks with:

```bash
find runtime/validation/<study>/chunks -maxdepth 2 -name summary.json | wc -l
```

Historical chunks can be rerun cleanly. `validate-study` reuses completed chunk
summaries unless you pass `--force`.

### Other Run Examples

```bash
# Use a different weather model
./deploy/gcp/mwn.sh run --model NBM --hours 12

# Long-range GFS forecast (global coverage)
./deploy/gcp/mwn.sh run --model GFS --hours 48

# Historical analysis of past wind conditions
./deploy/gcp/mwn.sh run --mode reanalysis --hours 12

# Quick single-wind scenario (no internet needed)
./deploy/gcp/mwn.sh run --mode domain-average --speed 20 --direction 270

# Keep raw output files for inspection
./deploy/gcp/mwn.sh run --hours 6 --keep-temp
```

### Run Options Cheat Sheet

Common `run` options:

| Option | Use |
|--------|-----|
| `--mode forecast` | Current/future weather-model run, default |
| `--mode reanalysis` | Historical HRRR pastcast run |
| `--mode domain-average` | Manual speed/direction, no weather download |
| `--model HRRR` | Default high-resolution CONUS model; also native historical model |
| `--model NBM` | Forecast-only blended guidance |
| `--model NAM` | Forecast-only NAM nest guidance |
| `--model RAP` | Forecast-only rapid-refresh guidance |
| `--model GFS` | Forecast-only long-range global guidance |
| `--weather-source native` | Default WindNinja NOMADS/GCP weather path |
| `--weather-source herbie` | Opt-in Herbie forecast-file path; requires rebuilt image |
| `--hours N` | Forecast/reanalysis duration when not using exact start/end |
| `--start UTC --end UTC` | Exact reanalysis window; both must be hour-aligned UTC |
| `--domain KEY` | Domain from `config/domains.json` |
| `--height N` | Output wind height above ground in meters |
| `--keep-temp` | Keep raw rasters/KMZ/config under `runtime/temp/` |
| `--no-upload` | Skip GCS upload even if configured |
| `--dry-run` | Generate config without running WindNinja |
| `--speed N --direction N` | Required with `--mode domain-average` |
| `--speed-units mph\|mps\|kph\|kts` | Units for domain-average speed |

Useful environment settings in `config/runtime.env`:

| Setting | Use |
|---------|-----|
| `MWN_DOMAIN_ID` | Default domain when `--domain` is omitted |
| `MWN_DOCKER_IMAGE` | Image used by Docker Compose |
| `MWN_NUM_THREADS` | Overrides WindNinja `num_threads`; keep near physical CPU count |
| `MWN_GCS_UPLOAD_ENABLED` | Enables upload after successful runs |
| `MWN_GCS_BUCKET` | Upload destination bucket |
| `MWN_HERBIE_CACHE` | Local cache for Herbie GRIB subsets and WindNinja NetCDF files |
| `MWN_HERBIE_PRIORITY` | Herbie source priority, for example `aws,google,azure,nomads` |
| `MWN_SYNOPTIC_TOKEN` | Required for Synoptic validation |
| `CUSTOM_SRTM_API_KEY` | Required for SRTM DEM downloads |

Herbie forecast support is image-level because it adds `herbie-data`, `xarray`,
`cfgrib`, `netCDF4`, and the ecCodes runtime library. After pulling code that
adds or changes that path, rebuild with:

```bash
./deploy/gcp/mwn.sh build-local
```

Do a native smoke test first, then a Herbie opt-in smoke:

```bash
./deploy/gcp/mwn.sh smoke --keep-temp
./deploy/gcp/mwn.sh run --weather-source herbie --model HRRR --hours 1 --keep-temp --no-upload
```

## Step 9: Get Your Output

Output files are in `runtime/` on the VM:

- `runtime/archives/` -- zipped run outputs (KMZ files, ASCII grids, generated config)
- `runtime/temp/` -- raw output if you used `--keep-temp`

To download files to your local machine:

```bash
# From your local terminal
gcloud compute scp windninja:/opt/mountain_windninja/runtime/archives/*.zip ~/Downloads/
```

Open the KMZ files in [Google Earth](https://earth.google.com/web/) to see the wind simulation.

## Step 10: Stop Or Clean Up

Stop the VM whenever you are done working:

```bash
exit
gcloud compute instances stop windninja --zone us-central1-a
```

Clean generated mesh caches and raw temp output before a handoff or after a
failed OpenFOAM mesh run:

```bash
./deploy/gcp/mwn.sh clean
```

`clean` preserves `config/runtime.env`, terrain inputs under `static_data/`, and
validation summaries under `runtime/validation/`.

Do not run cleanup while a validation container is active. Check `docker ps` and
any active `tmux` or `screen` session first.

## Optional: GCS Upload

If you want results uploaded to a public bucket:

1. Create a bucket in [Cloud Storage](https://console.cloud.google.com/storage)
2. Give your VM's service account "Storage Object Admin" permission on the bucket
3. Edit `config/runtime.env`:

```
MWN_GCS_BUCKET=your-bucket-name
MWN_GCS_UPLOAD_ENABLED=true
```

1. Run a forecast -- results upload automatically

## Optional: Automatic Scheduling

To run forecasts automatically every hour:

```bash
./deploy/gcp/mwn.sh schedule
```

This starts a background container that runs an 18-hour HRRR forecast at :15 past every hour. To stop it:

```bash
./deploy/gcp/mwn.sh stop
```

View logs:

```bash
./deploy/gcp/mwn.sh logs
```

## Troubleshooting

### Setup Problems

**Build fails with disk space error:**
You need at least 50 GB disk. Check with `df -h`. If low, resize the boot disk in the GCP Console (Compute Engine > Disks > Edit) and then run:

```bash
sudo growpart /dev/sda 1
sudo resize2fs /dev/sda1
```

**Permission denied on Docker:**
Log out and back in after running `bootstrap_repo.sh`. Docker group membership requires a new session:

```bash
exit
# SSH back in
cd /opt/mountain_windninja
docker ps   # should work without sudo now
```

If it still fails: `sudo usermod -aG docker $USER` then log out/in again.

**"Domain not in domains.json":**
The `MWN_DOMAIN_ID` in `config/runtime.env` must exactly match a key in `config/domains.json`. Check for typos:

```bash
cat config/runtime.env | grep MWN_DOMAIN_ID
cat config/domains.json
```

### Terrain/DEM Problems

**fetch-dem fails with "API Key required":**
OpenTopography requires a free API key (since 2022). Get one at [opentopography.org](https://opentopography.org/) (create account > Dashboard > Request API Key). Add it to `config/runtime.env`:

```
CUSTOM_SRTM_API_KEY=your_key_here
```

**fetch-dem fails with "could not download" or timeout:**
OpenTopography servers can be slow. Try again in a few minutes. If it keeps failing, download the DEM manually from [USGS National Map](https://apps.nationalmap.gov/downloader/) or [OpenTopography portal](https://portal.opentopography.org/) and upload it to your VM.

**fetch-lcp is very slow (5-15 minutes):**
This is normal. LANDFIRE's server generates the LCP file on demand. For large areas it can take up to 15 minutes. If it times out, the LANDFIRE server may be under heavy load -- try again later or use their [web viewer](https://www.landfire.gov/viewer/) to download manually.

**WindNinja crashes immediately with "Error reading elevation file":**
Your terrain file likely has one of these issues:

1. **Wrong projection.** WindNinja requires a projected CRS (like UTM), not geographic lat/lon. Check with:

```bash
./deploy/gcp/mwn.sh shell
gdalinfo static_data/your_file.tif | grep "Coordinate System"
```

If it says `GEOGCS` (geographic), you need to reproject it:

```bash
gdalwarp -t_srs EPSG:32613 static_data/your_file.tif static_data/your_file_utm.tif
```

Replace `32613` with the correct UTM zone for your area. Google "UTM zone map" to find yours.

1. **Not north-up.** The DEM must be oriented north-up. Most downloads are, but if yours was rotated, reproject it with `gdalwarp`.
2. **No-data gaps.** Holes in the terrain data cause crashes. Fill them with:

```bash
./deploy/gcp/mwn.sh shell
gdal_fillnodata.py static_data/your_file.tif static_data/your_file_filled.tif
```

1. **Elevation not in meters.** Some DEMs use feet. Convert with:

```bash
gdal_calc.py -A static_data/your_file.tif --outfile=static_data/your_file_m.tif --calc="A*0.3048"
```

**LCP file hangs at "Reading elevation file":**
Known issue with `griddedInitialization` + LCP files in some WindNinja versions. If using an LCP, make sure you're using `wxModelInitialization` (forecast mode) or `domainAverageInitialization` (domain-average mode), not gridded initialization.

### Weather Model / Forecast Problems

**"Could not download weather data" or NOAA timeout:**
NOAA's NOMADS servers go down occasionally for maintenance or high load. Steps to fix:

1. **Wait and retry.** Most outages resolve within 30 minutes.
2. **Try a different model.** If HRRR fails, try `--model GFS` (different server):

```bash
./deploy/gcp/mwn.sh run --model GFS --hours 6
```

1. **Increase download timeout.** Add to `config/runtime.env`:

```
NOMADS_HTTP_TIMEOUT=60
```

Default is 20 seconds, which can be too short on slow connections.

1. **Check NOAA status.** NOAA posts outage notices at: [lstsrv.ncep.noaa.gov/mailman/listinfo/ncep.list.nomads-ftpprd](https://lstsrv.ncep.noaa.gov/mailman/listinfo/ncep.list.nomads-ftpprd)

**Reanalysis mode fails:**
Historical (pastcast) data is only available for HRRR and only goes back to ~2014. Make sure you're using `--model HRRR` with reanalysis mode. Other models don't have pastcast archives.
If you just pulled a change that updates `Dockerfile`, rebuild once with `./deploy/gcp/mwn.sh build` before retrying historical runs. The patched image can read public HRRR archive data without manual GCS keys.
If you still see `Missing required GCS credentials`, you are almost certainly running an older image layer. Pull latest code, rebuild, then retry.

**Synoptic validation fails with 403 / Unauthorized:**
The validation workflow needs a Synoptic token with actual data access, not just a syntactically valid token value in `config/runtime.env`. Verify access in the Synoptic customer console before debugging the repo code further.

### OpenFOAM / Momentum Solver Problems

These issues only apply when using `momentum_flag = true` in your config template (the default in this project).

**"Error during blockMesh" or "posix_spawnp() failed":**
This is the most common Docker issue. OpenFOAM's MPI requires root permissions in Docker. Our `run_windninja.sh` script sets the right environment variables, but if you're running WindNinja manually inside the container, you need:

```bash
source /opt/openfoam9/etc/bashrc
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
export FOAM_USER_LIBBIN=/usr/local/lib/
```

**"Error during reconstructPar" with multiple threads:**
Same root cause as above. Additionally, set:

```bash
export OMPI_MCA_btl_vader_single_copy_mechanism=none
```

This fixes an MPI shared-memory issue in Docker containers.

**Stuck at "moveDynamicMesh 100% complete" (never finishes):**
This is a known OpenFOAM MPI bug in containers. Fix by adding this environment variable before running:

```bash
export OMPI_MCA_btl_vader_single_copy_mechanism=none
```

Our Docker setup should handle this, but if you see it, add the variable to `config/runtime.env`.

**"Error during decomposePar" with "Essential entry 'value' missing":**
This means the wrong OpenFOAM version is being used. This repo's Docker image
uses OpenFOAM 9. If you built OpenFOAM separately, make sure it matches the repo
image. In our Docker container, this should not happen.

**Momentum solver diverges or produces garbage output:**
Usually caused by a rough or noisy DEM. WindNinja v3.11+ has a built-in DEM smoothing algorithm. You can also try:

- Set `NINJA_FILL_DEM_NO_DATA=YES` in your environment to fill data gaps
- Increase `mesh_resolution` in the template (e.g., from 80 to 200) to use a coarser grid
- Reduce `number_of_iterations` from 300 to 200

### Performance Problems

**Simulation takes too long:**
Several levers to speed things up, ordered by impact:

1. **Turn off momentum solver:** Set `momentum_flag = false` in your `.cfg` template. Mass-only solver runs in seconds instead of minutes. Less accurate for complex terrain but fine for gentle topography.
2. **Increase mesh resolution number:** Counter-intuitively, a larger `mesh_resolution` value means coarser (faster) grid. Change from `80.0` to `200.0` in the template.
3. **Reduce forecast hours:** `--hours 6` instead of `--hours 18`.
4. **Use fewer threads on small domains:** For small DEMs, `num_threads = 2` can be faster than 4 due to MPI overhead.
5. **Shrink your DEM:** Crop to only the area you need:

```bash
./deploy/gcp/mwn.sh shell
gdal_translate -projwin west north east south static_data/big.tif static_data/small.tif
```

**Expected run times** (rough guide for e2-standard-4, 4 vCPUs):


| Domain Size       | Solver    | Per Forecast Hour |
| ----------------- | --------- | ----------------- |
| Small (10x10 km)  | Mass only | ~5 seconds        |
| Small (10x10 km)  | Momentum  | ~2-3 minutes      |
| Medium (30x30 km) | Mass only | ~15 seconds       |
| Medium (30x30 km) | Momentum  | ~8-15 minutes     |
| Large (50x50 km)  | Momentum  | ~20-40 minutes    |


An 18-hour HRRR forecast on a medium domain with momentum solver takes roughly 2.5-4.5 hours on e2-standard-4.

### Debug Mode

If something fails and the error message isn't clear, enable debug output:

```bash
# Add to config/runtime.env:
CPL_DEBUG=NINJAFOAM

# Then re-run your forecast
./deploy/gcp/mwn.sh run --hours 6 --keep-temp
```

This produces verbose output showing exactly where WindNinja fails. The `--keep-temp` flag keeps the raw output directory so you can inspect it.

## Stopping the VM

When you're done, **always stop the VM** from the GCP Console (or via CLI) to avoid charges. Your data persists across stops -- `/opt/mountain_windninja/runtime/`, `static_data/`, and the Docker image all stay on the disk.

```bash
# From your local terminal (if you have gcloud installed)
gcloud compute instances stop windninja --zone us-central1-a

# To start it again later
gcloud compute instances start windninja --zone us-central1-a
```

Or from the GCP Console: go to **Compute Engine > VM instances**, check the box next to your VM, and click **Stop** at the top.

---

## Managing Your Costs

This section covers practical strategies to keep your GCP bill low. WindNinja is a batch workload (you run it, get results, and stop), so you should rarely need to pay more than a few dollars per month.

### The #1 Rule: Stop Your VM When You're Done

A stopped VM costs almost nothing (~$5/month for 50 GB disk storage). A running e2-standard-4 costs $0.134/hour, which is $97/month if left on 24/7. **Stopping the VM when you're done is the single biggest cost saver.**

Make it a habit: run your simulation, download results, stop the VM. Here's a one-liner that runs a forecast and then stops the VM automatically:

```bash
./deploy/gcp/mwn.sh run --hours 18 && sudo shutdown -h now
```

The `shutdown -h now` command stops the VM after the forecast completes, so you don't forget.

### Setting Budget Alerts

Set up a budget alert so you get an email if spending goes above a threshold. This is free and takes 2 minutes:

1. Go to [console.cloud.google.com/billing](https://console.cloud.google.com/billing)
2. Click your billing account
3. Click **Budgets & alerts** in the left sidebar
4. Click **Create Budget**
5. Name it "WindNinja", select your project
6. Set the budget amount to something you're comfortable with (e.g., **$25**)
7. Leave the default alert thresholds (50%, 90%, 100%)
8. Click **Finish**

You'll get an email when you hit 50%, 90%, and 100% of your budget. This is just an alert -- it does not stop spending. But it prevents surprises.

### Scheduling Auto-Stop (Prevent Forgotten VMs)

If you're worried about forgetting to stop your VM, set up an instance schedule that automatically stops it:

```bash
# Create a schedule that stops the VM every day at 11 PM
gcloud compute resource-policies create instance-schedule windninja-autostop \
  --region=us-central1 \
  --vm-stop-schedule="0 23 * * *" \
  --timezone="America/Denver"

# Attach the schedule to your VM
gcloud compute instances add-resource-policies windninja \
  --resource-policies=windninja-autostop \
  --zone=us-central1-a
```

This way, even if you forget, the VM shuts down at 11 PM Mountain time every night. You can always manually start it again when you need it. Change the timezone and hour to match your local schedule.

### Using Your $300 Free Credit Wisely

New GCP accounts get $300 in free credits for 90 days. Here's how to stretch them:


| What                      | Cost      | Hours You Can Run            |
| ------------------------- | --------- | ---------------------------- |
| e2-standard-4 (on-demand) | $0.134/hr | ~2,238 hours ($300 / $0.134) |
| e2-standard-4 (Spot)      | $0.065/hr | ~4,615 hours                 |
| c2-standard-8 (on-demand) | $0.418/hr | ~717 hours                   |


At 2 hours of runtime per day on e2-standard-4, your $300 lasts about **3 years** of daily use (well beyond the 90-day credit window). In practice, a typical WindNinja forecast run takes 15-60 minutes depending on domain size.

### GCP Free Tier (After Credits Run Out)

After your $300 expires, GCP offers a permanent "Always Free" tier:

- **1 e2-micro instance** per month (us-central1, us-east1, or us-west1) -- too small for WindNinja but useful for a lightweight management server
- **5 GB Cloud Storage** -- enough to store several sets of output files
- **30 GB persistent disk** -- not quite enough for WindNinja's Docker image (you need 50 GB)

The free tier won't run WindNinja directly, but you can keep your results in Cloud Storage for free. For actual runs, you'll need to pay, but at $0.134/hour it's very affordable.

### Cost Comparison: GCP vs. Other Options


| Option                   | Cost for 4-CPU, 16 GB          | Notes                                |
| ------------------------ | ------------------------------ | ------------------------------------ |
| GCP e2-standard-4        | $0.134/hr (~$16/mo at 4hr/day) | Easy setup, free credits             |
| GCP e2-standard-4 Spot   | $0.065/hr (~$8/mo at 4hr/day)  | 50% cheaper, can be interrupted      |
| AWS EC2 t3.xlarge        | $0.166/hr                      | Similar, no free $300 credit         |
| Hetzner CPX31            | ~$0.03/hr (flat rate)          | Cheapest but requires more setup     |
| Local desktop (existing) | $0 (electricity only)          | No cloud cost; Linux/Docker required |


GCP's $300 free credit makes it the best starting point. Hetzner is cheapest long-term but has no managed SSH or console.

### Monitoring Spending

Check your current spend at any time:

```bash
# Quick view of current charges
gcloud billing accounts list

# Or go to the Console
# console.cloud.google.com/billing
```

The **Billing > Reports** page in the Console shows a daily spending graph broken down by service. Check it weekly to make sure there are no surprises.

### Quick Setup With gcloud CLI (Optional)

If you want to manage your VM from your local terminal instead of the browser console, install the `gcloud` CLI:

1. Download from [cloud.google.com/sdk/install](https://cloud.google.com/sdk/install)
2. Run `gcloud init` and follow the prompts to log in and select your project
3. Now you can do everything from your terminal:

```bash
# Create the VM (one-time)
gcloud compute instances create windninja \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-balanced

# SSH in
gcloud compute ssh windninja --zone=us-central1-a

# Stop when done
gcloud compute instances stop windninja --zone=us-central1-a

# Start again later
gcloud compute instances start windninja --zone=us-central1-a

# Download results
gcloud compute scp windninja:/opt/mountain_windninja/runtime/archives/*.zip ~/Downloads/
```

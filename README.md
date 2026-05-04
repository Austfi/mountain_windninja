# Mountain WindNinja

Run [WindNinja](https://research.fs.usda.gov/firelab/products/dataandtools/windninja)
from a Docker-based command line workflow. The wrapper downloads terrain, registers
simulation domains, runs WindNinja inside a container, and writes Google Earth KMZ
plus ASCII grid output.

## First Successful Run

You need one latitude/longitude point near the center of the area you want to
simulate. A 10-12 km square is a good first domain. Docker must already be
installed and running.

```bash
git clone https://github.com/Austfi/mountain_windninja.git /opt/mountain_windninja
cd /opt/mountain_windninja

# Create runtime/, static_data/, config/runtime.env, and pull the default image.
./deploy/gcp/mwn.sh init

# Download DEM + LCP terrain, register a domain, and make it the default.
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain my_area \
  --label "My Area"

# Check configuration and terrain.
./deploy/gcp/mwn.sh check

# Run a deterministic one-hour domain-average test.
./deploy/gcp/mwn.sh smoke

# Run a short HRRR forecast using NOAA weather input.
./deploy/gcp/mwn.sh run --hours 6
```

Output archives are written to `runtime/archives/`. Each zip contains the generated
WindNinja config, KMZ files for Google Earth, and ASCII wind grids. Use `--keep-temp`
on `smoke` or `run` when you want raw output left under `runtime/temp/`.

## Docker Images

`mwn.sh init` tries to pull the published GHCR image:

```bash
./deploy/gcp/mwn.sh pull ghcr.io/austfi/mountain-windninja:3.12.2
```

If the pull fails or you are changing image-level dependencies, build locally:

```bash
./deploy/gcp/mwn.sh build-local
```

The first local build takes about 30 minutes.

## Common Commands

| Command | Purpose |
|---------|---------|
| `mwn.sh init` | Create local runtime directories and `config/runtime.env` if missing |
| `mwn.sh fetch-terrain ... --domain KEY` | Download DEM + LCP terrain and register a domain |
| `mwn.sh check [--domain KEY]` | Run preflight checks |
| `mwn.sh smoke [--domain KEY]` | Run a fixed `10 mph`, `270 deg`, one-hour smoke test |
| `mwn.sh run --hours N` | Run a forecast |
| `mwn.sh run --mode reanalysis ...` | Run historical HRRR reanalysis |
| `mwn.sh validate-study berthoud_pass ...` | Run chunked HRRR/WindNinja/Synoptic validation |
| `mwn.sh clean` | Clear cached OpenFOAM mesh and temp output |

Run `./deploy/gcp/mwn.sh help` for the beginner command list, or
`./deploy/gcp/mwn.sh help advanced` for lower-level tools.

## Terrain Sources

Terrain sources:

| Source | Coverage | Notes |
|--------|----------|-------|
| `us` | United States | USGS 3DEP DEM, no API key |
| `srtm` | Global between 60N and 56S | Requires `CUSTOM_SRTM_API_KEY` |
| `gmted` | Global | Coarse, useful for large domains |
| `lcp` | United States | LANDFIRE landscape file with vegetation/fuel bands |

Examples:

```bash
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain keystone \
  --label "Keystone"

./deploy/gcp/mwn.sh fetch-terrain --area-file keystone.kml --padding-km 1 \
  --domain keystone \
  --label "Keystone"

./deploy/gcp/mwn.sh fetch-dem 45.5 7.0 45.0 6.5 static_data/alps.tif srtm 30 \
  --domain alps \
  --label "Alps" \
  --no-set-default

./deploy/gcp/mwn.sh domain create keystone \
  --bbox 39.65 -106.0 39.55 -106.15 \
  --terrain-source us \
  --resolution 10
```

## Docs

- [Quickstart](docs/quickstart.md) - first successful run
- [Command reference](docs/commands.md) - every command and flag
- [Terrain guide](docs/terrain.md) - DEM, LCP, source selection, and troubleshooting
- [Scheduling guide](docs/scheduling.md) - automatic forecast runs
- [Validation guide](docs/validation.md) - Synoptic and raster validation workflow
- [Development guide](docs/development.md) - local Python/dev overrides
- [GCP setup guide](docs/gcp_setup.md) - VM sizing, cost notes, and operations
- [WindNinja reference](docs/windninja_reference.md) - upstream config details

## Operational Notes

`config/domains.json` is a local domain catalog. The starter entries are examples;
`fetch-terrain` downloads both `KEY.tif` and `KEY.lcp` under `MWN_STATIC_DATA_ROOT`
(default `static_data/`).
The domain uses the LCP when both downloads succeed; the DEM remains available
as a fallback. `fetch-dem --domain`, `fetch-lcp --domain`, and `domain create`
remain available for advanced/manual terrain workflows.

Changes under `scripts/`, `config/`, and `docker/` are bind-mounted into the
container for normal runs. Rebuild the image after changes to `Dockerfile`,
dependency lists, or compiled WindNinja/OpenFOAM/GDAL behavior.

If a run fails with `moveDynamicMesh` or `Can't open log.ninja`, clear the mesh
cache:

```bash
./deploy/gcp/mwn.sh clean
```

For historical validation, do not pass `--points-file` to momentum runs. Run
reanalysis normally, keep the temp directory, then use `validate-rasters`.
For Berthoud Pass validation, use the higher-level study wrapper:

```bash
./deploy/gcp/mwn.sh validate-study berthoud_pass \
  --start 202601010000 \
  --pilot-hours 3
```

The study command reads explicit stations from
`config/stations/berthoud_pass_validation_manifest.csv`, runs HRRR reanalysis in
chunks, validates WindNinja and parent HRRR rasters, and writes aggregate outputs
under `runtime/validation/berthoud_pass/`.
The Berthoud manifest includes K0CO and CABTP. CABTP's wind sensor height is not
confirmed in this repo; leave `height_m_override` blank unless the actual
anemometer height is known. The plot command writes a station-location SVG
beside the comparison plots.

Generated runtime output, terrain downloads, caches, and local runtime config are
ignored by git. Source inputs such as `config/stations/*.csv` and
`config/studies/*.json` are intentionally trackable.

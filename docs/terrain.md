# Terrain Guide

WindNinja needs a terrain surface for each domain. The beginner path is
`fetch-terrain --center ... --size-km ... --domain`, which downloads both a DEM
and an LCP, then registers the domain in `config/domains.json`:

```bash
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 \
  --domain my_area \
  --label "My Area"
```

`fetch-terrain` saves `KEY.tif` as the DEM fallback and `KEY.lcp` as the active
terrain under `MWN_STATIC_DATA_ROOT` (default `static_data/`). If both succeed,
the domain uses the LCP.

Area input options:

| Input | Use When | Example |
|-------|----------|---------|
| `--center LAT LON --size-km N` | Beginner default | `--center 39.60 -106.08 --size-km 12` |
| `N E S W` | You know exact bounds | `39.65 -106.0 39.55 -106.15` |
| `--area-file area.kml` | You drew or exported an area | `--area-file area.kml --padding-km 1` |

Center/size boxes are limited to 50 km. Use explicit bbox for larger advanced
domains.

## Sources

| Source | Output | Coverage | Notes |
|--------|--------|----------|-------|
| `us` | DEM `.tif` | United States | USGS 3DEP, no API key |
| `srtm` | DEM `.tif` | Global between 60N and 56S | Requires `CUSTOM_SRTM_API_KEY` |
| `gmted` | DEM `.tif` | Global | Coarse, useful for large domains |
| `lcp` | LCP `.lcp` plus `.prj` | United States | LANDFIRE landscape file with vegetation/fuel bands |

Default output paths are `MWN_STATIC_DATA_ROOT/KEY.tif` for DEM and
`MWN_STATIC_DATA_ROOT/KEY.lcp` for LCP when `--domain KEY` is provided.

## DEM Or LCP

Use `fetch-terrain` for normal US domains. It gives you both the simple DEM
fallback and the vegetation-aware LCP active terrain.

Use `fetch-dem` alone for non-US domains or when LANDFIRE is unavailable.
WindNinja applies the uniform vegetation setting from `MWN_SURFACE_VEGETATION`
for DEM-only runs.

Use `fetch-lcp` alone when the DEM already exists and you only need to refresh
LANDFIRE data. LCP files need a `.prj` sidecar; `fetch-terrain`, `fetch-lcp`,
and `domain create --terrain-source lcp` generate it automatically.

## Existing Terrain Files

If you already have a file in `static_data/`, register it directly:

```bash
./deploy/gcp/mwn.sh fetch-dem --center 39.60 -106.08 --size-km 12 static_data/my_area.tif \
  --domain my_area --label "My Area"
```

For a LANDFIRE GeoTIFF with landscape bands:

```bash
./deploy/gcp/mwn.sh lcp-build static_data/source/landscape.tif static_data/my_area.lcp
```

Then register `static_data/my_area.lcp` with `fetch-lcp --domain` or by editing
`config/domains.json`.

## More Detail

- [Quickstart](quickstart.md) - first domain creation
- [Command reference](commands.md#fetch-dem) - all `fetch-dem` flags
- [GCP setup guide terrain section](gcp_setup.md#step-6-get-your-terrain-data) -
  manual terrain workflows and troubleshooting

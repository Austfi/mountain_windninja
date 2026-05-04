# Tutorial

This page now points to the shorter beginner path. Use
[quickstart.md](quickstart.md) for the first successful run:

```bash
./deploy/gcp/mwn.sh init
./deploy/gcp/mwn.sh fetch-terrain --center 39.60 -106.08 --size-km 12 --domain my_area --label "My Area"
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh smoke
./deploy/gcp/mwn.sh run --hours 6
```

After that:

- [Terrain guide](terrain.md) covers DEM/LCP choices.
- [Command reference](commands.md) covers every CLI flag.
- [Validation guide](validation.md) covers Synoptic and raster validation.
- [Scheduling guide](scheduling.md) covers automated runs.
- [GCP setup guide](gcp_setup.md) covers VM setup and operations.

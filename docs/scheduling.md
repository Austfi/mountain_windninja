# Scheduling Guide

The scheduler runs forecasts in a background Docker container. Use it only after
manual `check`, `smoke`, and `run --hours 6` pass for the target domain.

## Configure

Edit `config/runtime.env`:

```env
MWN_SCHEDULE_MINUTE=15
MWN_SCHEDULE_MODE=forecast
MWN_SCHEDULE_MODEL=HRRR
MWN_SCHEDULE_HOURS=18
```

The scheduler uses the active `MWN_DOMAIN_ID`.

## Start And Stop

```bash
./deploy/gcp/mwn.sh schedule
./deploy/gcp/mwn.sh logs
./deploy/gcp/mwn.sh stop
```

Outputs are written to `runtime/archives/`. Uploads happen only when
`MWN_GCS_UPLOAD_ENABLED=true`.

## Operational Notes

- Prefer standard VMs for scheduled production runs. Spot VMs can stop mid-run.
- If a scheduled run fails with mesh errors, stop the scheduler, run
  `./deploy/gcp/mwn.sh clean`, then smoke test manually.
- If code changes touch `Dockerfile` or dependencies, rebuild before restarting
  the scheduler.

## More Detail

- [Command reference](commands.md#schedule)
- [GCP setup guide](gcp_setup.md)
- [Agent handoff](agent_handoff.md)

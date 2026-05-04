from __future__ import annotations

import subprocess

import scripts.hourly_run as hourly_run


def test_run_daily_for_cycle_passes_configured_hours(monkeypatch):
    captured = {}

    def fake_run(cmd, capture_output, text):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout="ok\n", stderr="")

    monkeypatch.setattr(hourly_run.subprocess, "run", fake_run)

    assert hourly_run.run_daily_for_cycle("reanalysis", "HRRR", 6) is True

    assert captured["cmd"][-6:] == [
        "--mode", "reanalysis",
        "--model", "HRRR",
        "--hours", "6",
    ]


def test_run_daily_for_cycle_reports_failure(monkeypatch):
    def fake_run(cmd, capture_output, text):
        return subprocess.CompletedProcess(cmd, 2, stdout="", stderr="failed")

    monkeypatch.setattr(hourly_run.subprocess, "run", fake_run)

    assert hourly_run.run_daily_for_cycle("forecast", "HRRR", 18) is False


def test_main_returns_failure_when_scheduled_run_fails(monkeypatch):
    monkeypatch.setattr(hourly_run, "run_daily_for_cycle", lambda *args, **kwargs: False)
    monkeypatch.setattr(hourly_run.sys, "argv", ["hourly_run.py"])
    monkeypatch.setenv("MWN_SCHEDULE_MODE", "forecast")
    monkeypatch.setenv("MWN_SCHEDULE_MODEL", "HRRR")
    monkeypatch.setenv("MWN_SCHEDULE_HOURS", "18")

    assert hourly_run.main() == 1


def test_main_uses_schedule_env(monkeypatch):
    captured = {}

    def fake_run_daily_for_cycle(mode, model, hours, dry_run=False):
        captured.update({
            "mode": mode,
            "model": model,
            "hours": hours,
            "dry_run": dry_run,
        })
        return True

    monkeypatch.setattr(hourly_run, "run_daily_for_cycle", fake_run_daily_for_cycle)
    monkeypatch.setattr(hourly_run.gcs_manager, "update_index", lambda: None)
    monkeypatch.setattr(hourly_run.config_loader, "GCS_UPLOAD_ENABLED", False)
    monkeypatch.setattr(hourly_run.sys, "argv", ["hourly_run.py", "--dry-run"])
    monkeypatch.setenv("MWN_SCHEDULE_MODE", "reanalysis")
    monkeypatch.setenv("MWN_SCHEDULE_MODEL", "HRRR")
    monkeypatch.setenv("MWN_SCHEDULE_HOURS", "24")

    assert hourly_run.main() == 0
    assert captured == {
        "mode": "reanalysis",
        "model": "HRRR",
        "hours": 24,
        "dry_run": True,
    }

from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path

from scripts import validation_study as vs


UTC = dt.timezone.utc


def test_load_berthoud_study_config():
    study = vs.load_study_config("berthoud_pass")

    assert study.key == "berthoud_pass"
    assert study.domain == "berthoud_pass"
    assert study.model == "HRRR"
    assert study.chunk_hours == 24
    assert study.station_manifest.name == "berthoud_pass_validation_manifest.csv"


def test_plan_chunks_splits_exact_hours():
    chunks = vs.plan_chunks(
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        dt.datetime(2026, 1, 4, 6, 0, tzinfo=UTC),
        24,
    )

    assert [chunk.hours for chunk in chunks] == [24, 24, 24, 6]
    assert chunks[0].label == "20260101_0000_20260102_0000"
    assert chunks[-1].label == "20260104_0000_20260104_0600"


def test_run_dir_for_chunk_uses_daily_run_naming():
    study = vs.load_study_config("berthoud_pass")
    chunk = vs.Chunk(
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        dt.datetime(2026, 1, 1, 3, 0, tzinfo=UTC),
    )

    run_dir = vs.run_dir_for_chunk(study, chunk)

    assert run_dir.name == "berthoud_pass_20260101_0000_reanalysis_3h_HRRR"


def test_run_dir_for_nbm_uses_archive_naming():
    study = vs.with_overrides(
        vs.load_study_config("berthoud_pass"),
        type("Args", (), {
            "domain": None,
            "model": "NBM",
            "chunk_hours": None,
            "tolerance_minutes": None,
            "speed_units": None,
            "default_height": None,
            "lead_hours": 1,
        })(),
    )
    chunk = vs.Chunk(
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        dt.datetime(2026, 1, 1, 3, 0, tzinfo=UTC),
    )

    run_dir = vs.run_dir_for_chunk(study, chunk)

    assert run_dir.name == "berthoud_pass_20260101_0000_nbm_archive_3h_NBM"


def _write_sample_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_aggregate_outputs_combines_chunk_samples(tmp_path):
    study = vs.StudyConfig(
        key="test",
        label="Test",
        domain="test_domain",
        model="HRRR",
        chunk_hours=24,
        tolerance_minutes=30,
        speed_units="mph",
        default_height_m=10.0,
        padding_km=2.0,
        validation_root=tmp_path,
        station_manifest=tmp_path / "stations.csv",
        metadata_file=tmp_path / "metadata.json",
        bbox_file=tmp_path / "bbox.json",
    )
    sample_paths = [tmp_path / "chunks" / "a" / "samples.csv"]
    rows = [{
        "station_id": "K0CO",
        "station_label": "Berthoud Pass AWOS",
        "group": "ridge",
        "sample_time_utc": "2026-01-01T00:00:00Z",
        "obs_time_utc": "2026-01-01T00:00:00Z",
        "obs_age_minutes": "0",
        "height_m": "10.0",
        "speed_obs": "20.0",
        "dir_obs_deg": "270.0",
        "u_obs": "20.0",
        "v_obs": "0.0",
        "wn_speed": "18.0",
        "wn_dir_deg": "265.0",
        "wn_u": "17.9",
        "wn_v": "1.6",
        "wn_speed_error": "-2.0",
        "wn_dir_abs_error_deg": "5.0",
        "wn_vector_error": "2.5",
        "wx_speed": "15.0",
        "wx_dir_deg": "250.0",
        "wx_u": "14.1",
        "wx_v": "5.1",
        "wx_speed_error": "-5.0",
        "wx_dir_abs_error_deg": "20.0",
        "wx_vector_error": "6.0",
    }]
    _write_sample_csv(sample_paths[0], rows)
    chunks = [
        vs.Chunk(
            dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        )
    ]

    vs.aggregate_outputs(study, chunks, sample_paths)

    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["matched_sample_count"] == 1
    assert summary["matched_station_count"] == 1
    assert summary["overall"]["windninja"]["vector_rmse"] == 2.5
    assert (tmp_path / "station_summary.csv").exists()
    assert (tmp_path / "group_summary.csv").exists()


def test_ensure_station_inputs_uses_explicit_manifest(monkeypatch, tmp_path):
    station_manifest = tmp_path / "stations.csv"
    station_manifest.write_text(
        "station_id,label,group,height_m_override\n"
        "K0CO,Berthoud Pass - Mines Peak AWOS,ridge,\n",
        encoding="utf-8",
    )
    study = vs.StudyConfig(
        key="test",
        label="Test",
        domain="test_domain",
        model="HRRR",
        chunk_hours=24,
        tolerance_minutes=30,
        speed_units="mph",
        default_height_m=10.0,
        padding_km=2.0,
        validation_root=tmp_path,
        station_manifest=station_manifest,
        metadata_file=tmp_path / "metadata.json",
        bbox_file=tmp_path / "bbox.json",
    )

    def fake_prepare_points(args):
        assert Path(args.station_file) == station_manifest
        Path(args.metadata_output).write_text('{"stations": []}', encoding="utf-8")
        Path(args.bbox_output).write_text("{}", encoding="utf-8")

    monkeypatch.setattr(vs.sv, "prepare_points", fake_prepare_points)

    vs.ensure_station_inputs(
        study,
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        dt.datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        "token",
    )

    rows = list(csv.DictReader(study.station_manifest.open(encoding="utf-8")))
    assert [row["station_id"] for row in rows] == ["K0CO"]


def test_main_plan_prints_without_running(capsys):
    result = vs.main([
        "berthoud_pass",
        "--start",
        "202601010000",
        "--pilot-hours",
        "3",
        "--plan",
    ])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["study"] == "berthoud_pass"
    assert payload["chunk_count"] == 1
    assert payload["chunks"][0]["end"] == "202601010300"


def test_nbm_archive_chunk_dry_run_uses_existing_validation_flow(monkeypatch, tmp_path):
    commands = []

    def fake_run_command(command, *, dry_run=False):
        commands.append(command)
        assert dry_run is True

    monkeypatch.setattr(vs, "run_command", fake_run_command)
    monkeypatch.setattr(vs.config_loader, "RUNTIME_DIR", tmp_path / "runtime")
    monkeypatch.setattr(vs.config_loader, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(vs.config_loader, "SCRIPTS_DIR", tmp_path / "scripts")
    study = vs.StudyConfig(
        key="test",
        label="Test",
        domain="berthoud_pass",
        model="NBM",
        chunk_hours=1,
        tolerance_minutes=30,
        speed_units="mph",
        default_height_m=10.0,
        padding_km=2.0,
        validation_root=tmp_path / "validation",
        station_manifest=tmp_path / "stations.csv",
        metadata_file=tmp_path / "metadata.json",
        bbox_file=tmp_path / "bbox.json",
        lead_hours=1,
    )
    chunk = vs.Chunk(
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        dt.datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
    )

    run_dir = vs.run_reanalysis_chunk(study, chunk, force=False, dry_run=True)

    assert run_dir.name == "berthoud_pass_20260101_0000_nbm_archive_1h_NBM"
    assert len(commands) == 2
    assert commands[0][1].endswith("nbm_archive.py")
    assert commands[0][commands[0].index("--lead-hours") + 1] == "1"
    assert commands[1][1].endswith("gridded_run.py")

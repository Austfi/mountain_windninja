from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from ml.residual_unet.config import load_config
from ml.residual_unet.analyze_results import (
    metric_distribution,
    summarize_samples,
    summarize_training,
)
from ml.residual_unet.build_controlled_dataset import (
    controlled_split,
    infer_controlled_metadata,
    pair_manifest_rows,
)
from ml.residual_unet.build_combined_dataset import build_combined_dataset
from ml.residual_unet.build_dataset import (
    CHANNELS,
    LCP_CANOPY_CHANNELS,
    TARGETS,
    input_channels_for_features,
    terrain_features_from_input_channels,
)
from ml.residual_unet.controlled_pairs import (
    build_cases,
    build_solver_runs,
    mph_to_mps,
    profile_cases,
    write_config,
    write_run_script,
)
from ml.residual_unet.dataset import filter_rows
from ml.residual_unet.hrrr_pair_runs import (
    inference_command,
    is_complete_run,
    plan_runs,
    run_dir_for,
    runtime_env,
    windninja_output_pair_count,
)
from ml.residual_unet.infer import (
    _normalization_arrays,
    collect_inference_rasters,
)
from ml.residual_unet.pairing import (
    blocked_day_split,
    collect_windninja_rasters,
    pair_mass_momentum,
    parse_run_label,
)
from ml.residual_unet.prepare_colab_upload import upload_paths_to_gcs


def _write_empty(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_processed_sample(
    processed_dir: Path,
    sample_id: str,
    split: str,
    *,
    input_channels: list[str] | None = None,
) -> None:
    np = pytest.importorskip("numpy")

    channels = input_channels or CHANNELS
    sample_path = processed_dir / "samples" / f"{sample_id}.npz"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    valid_mask = np.ones((2, 2), dtype=np.bool_)
    np.savez_compressed(
        sample_path,
        x=np.ones((len(channels), 2, 2), dtype=np.float32),
        y=np.zeros((len(TARGETS), 2, 2), dtype=np.float32),
        mass_uv=np.ones((2, 2, 2), dtype=np.float32),
        mom_uv=np.ones((2, 2, 2), dtype=np.float32),
        valid_mask=valid_mask,
    )
    (processed_dir / "manifest.csv").write_text(
        "sample_id,split,npz_path\n"
        f"{sample_id},{split},samples/{sample_id}.npz\n",
        encoding="utf-8",
    )
    _write_json(
        processed_dir / "dataset_summary.json",
        {
            "sample_count": 1,
            "crop_size": 2,
            "input_channels": channels,
            "target_channels": TARGETS,
            "split_counts": {split: 1},
        },
    )


def test_upload_paths_to_gcs_uses_drive_upload_prefix(tmp_path, monkeypatch):
    first = tmp_path / "residual_unet_code.zip"
    second = tmp_path / "05_train_mountain_general_9p6_colab.ipynb"
    first.write_text("code", encoding="utf-8")
    second.write_text("notebook", encoding="utf-8")
    calls = []

    def fake_run(command, check):
        calls.append((command, check))

    monkeypatch.setattr("ml.residual_unet.prepare_colab_upload.subprocess.run", fake_run)

    upload_paths_to_gcs([first, second], "test-bucket", "drive_upload")

    assert calls == [
        (
            [
                "gcloud",
                "storage",
                "cp",
                str(first),
                "gs://test-bucket/drive_upload/residual_unet_code.zip",
            ],
            True,
        ),
        (
            [
                "gcloud",
                "storage",
                "cp",
                str(second),
                "gs://test-bucket/drive_upload/05_train_mountain_general_9p6_colab.ipynb",
            ],
            True,
        ),
    ]


def test_parse_run_label_supports_windninja_timestamp_forms():
    assert parse_run_label("01-05-2026_0200") == dt.datetime(
        2026,
        1,
        5,
        2,
        0,
        tzinfo=dt.timezone.utc,
    )
    assert parse_run_label("20260105_0200") == dt.datetime(
        2026,
        1,
        5,
        2,
        0,
        tzinfo=dt.timezone.utc,
    )


def test_collect_windninja_rasters_ignores_parent_hrrr_outputs(tmp_path):
    run_dir = tmp_path / "runtime" / "temp" / "berthoud_pass_20260105_0000_reanalysis_24h_HRRR"
    _write_empty(run_dir / "berthoud_pass_01-05-2026_0200_100m_vel.asc")
    _write_empty(run_dir / "berthoud_pass_01-05-2026_0200_100m_ang.asc")
    _write_empty(run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-01-05-2026_0200_vel.asc")
    _write_empty(run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-01-05-2026_0200_ang.asc")

    pairs = collect_windninja_rasters(run_dir)

    assert list(pairs) == [dt.datetime(2026, 1, 5, 2, 0, tzinfo=dt.timezone.utc)]
    assert pairs[next(iter(pairs))].speed_path.name == "berthoud_pass_01-05-2026_0200_100m_vel.asc"


def test_pair_mass_momentum_uses_existing_directory_names(tmp_path):
    temp_root = tmp_path / "runtime" / "temp"
    mom_dir = temp_root / "berthoud_pass_20260105_0000_reanalysis_24h_HRRR"
    mass_dir = temp_root / "berthoud_pass_mass_20260105_0000_reanalysis_24h_HRRR"
    for run_dir in (mom_dir, mass_dir):
        _write_empty(run_dir / "berthoud_pass_01-05-2026_0200_100m_vel.asc")
        _write_empty(run_dir / "berthoud_pass_01-05-2026_0200_100m_ang.asc")

    pairs = pair_mass_momentum(tmp_path)

    assert len(pairs) == 1
    assert pairs[0].mass.run_dir == mass_dir
    assert pairs[0].momentum.run_dir == mom_dir


def test_pair_mass_momentum_supports_custom_domains(tmp_path):
    temp_root = tmp_path / "runtime" / "temp"
    mom_dir = temp_root / "breck_tenmile_9p6_20260105_0000_reanalysis_24h_HRRR"
    mass_dir = temp_root / "breck_tenmile_9p6_mass_20260105_0000_reanalysis_24h_HRRR"
    for run_dir in (mom_dir, mass_dir):
        _write_empty(run_dir / "breck_tenmile_9p6_20260105_0200_100m_vel.asc")
        _write_empty(run_dir / "breck_tenmile_9p6_20260105_0200_100m_ang.asc")

    pairs = pair_mass_momentum(
        tmp_path,
        momentum_domain="breck_tenmile_9p6",
        mass_domain="breck_tenmile_9p6_mass",
    )

    assert len(pairs) == 1
    assert pairs[0].timestamp == dt.datetime(2026, 1, 5, 2, tzinfo=dt.timezone.utc)
    assert pairs[0].mass.run_dir == mass_dir


def test_blocked_day_split_keeps_whole_days_together():
    timestamps = [
        dt.datetime(2026, 1, day, hour, tzinfo=dt.timezone.utc)
        for day in range(1, 12)
        for hour in (0, 12)
    ]

    split_by_day = blocked_day_split(timestamps)

    assert split_by_day[dt.date(2026, 1, 1)] == "train"
    assert split_by_day[dt.date(2026, 1, 9)] == "val"
    assert split_by_day[dt.date(2026, 1, 10)] == "test"
    assert split_by_day[dt.date(2026, 1, 11)] == "train"


def test_wind_math_uses_meteorological_direction_convention():
    np = pytest.importorskip("numpy")
    from ml.residual_unet.wind_math import speed_direction_to_uv, uv_to_speed_direction

    u, v = speed_direction_to_uv(np.asarray([10.0]), np.asarray([0.0]), units="mps")

    assert u[0] == pytest.approx(0.0)
    assert v[0] == pytest.approx(-10.0)

    speeds, directions = uv_to_speed_direction(u, v, units="mps")
    assert speeds[0] == pytest.approx(10.0)
    assert directions[0] == pytest.approx(0.0)


def test_uv_to_speed_direction_round_trips_common_directions():
    np = pytest.importorskip("numpy")
    from ml.residual_unet.wind_math import speed_direction_to_uv, uv_to_speed_direction

    speed = np.asarray([10.0, 15.0, 20.0], dtype=np.float32)
    direction = np.asarray([90.0, 180.0, 225.0], dtype=np.float32)

    u, v = speed_direction_to_uv(speed, direction, units="mps")
    out_speed, out_direction = uv_to_speed_direction(u, v, units="mps")

    assert out_speed == pytest.approx(speed)
    assert out_direction == pytest.approx(direction)


def test_ascii_grid_write_read_round_trip_and_crop_metadata(tmp_path):
    np = pytest.importorskip("numpy")
    from ml.residual_unet.raster_io import (
        AsciiGrid,
        crop_grid_metadata,
        read_ascii_grid,
        write_ascii_grid,
    )

    grid = AsciiGrid(
        data=np.arange(20, dtype=np.float32).reshape(4, 5),
        ncols=5,
        nrows=4,
        xllcorner=100.0,
        yllcorner=200.0,
        cellsize=10.0,
        nodata=-9999.0,
    )

    cropped = crop_grid_metadata(grid, 2)
    output = tmp_path / "cropped.asc"
    write_ascii_grid(output, cropped)
    reread = read_ascii_grid(output)

    assert cropped.xllcorner == pytest.approx(110.0)
    assert cropped.yllcorner == pytest.approx(210.0)
    assert reread.ncols == 2
    assert reread.nrows == 2
    assert reread.data.tolist() == [[6.0, 7.0], [11.0, 12.0]]


def test_center_crop_handles_existing_berthoud_shape():
    np = pytest.importorskip("numpy")
    from ml.residual_unet.raster_io import center_crop

    values = np.zeros((5, 100, 101), dtype=np.float32)

    cropped = center_crop(values, 96)

    assert cropped.shape == (5, 96, 96)


def test_collect_inference_rasters_supports_timestamped_mass_outputs(tmp_path):
    run_dir = tmp_path / "run"
    _write_empty(run_dir / "berthoud_pass_mass_20260222_2000_100m_vel.asc")
    _write_empty(run_dir / "berthoud_pass_mass_20260222_2000_100m_ang.asc")
    _write_empty(run_dir / "HEIGHT-HRRR-20260222_2000_vel.asc")
    _write_empty(run_dir / "HEIGHT-HRRR-20260222_2000_ang.asc")

    pairs = collect_inference_rasters(run_dir)

    assert len(pairs) == 1
    assert pairs[0].sample_id == "berthoud_pass_mass_20260222_2000_100m"
    assert pairs[0].timestamp == dt.datetime(2026, 2, 22, 20, 0, tzinfo=dt.timezone.utc)


def test_inference_requires_checkpoint_normalization():
    with pytest.raises(ValueError, match="normalization"):
        _normalization_arrays({})


def test_load_config_reads_simple_nested_yaml():
    config = load_config("ml/residual_unet/configs/berthoud_v0.yaml")

    assert config["data"]["processed_dir"] == "ml/residual_unet/data/processed/berthoud_v0"
    assert config["model"]["in_channels"] == 5
    assert config["training"]["speed_loss_weight"] == 0.1


def test_analyze_results_summarizes_training_and_sample_spread():
    training = summarize_training([
        {
            "epoch": 1.0,
            "train_loss": 3.0,
            "val_loss": 2.0,
            "train_ml_vector_rmse": 1.5,
            "val_ml_vector_rmse": 1.0,
            "val_mass_vector_rmse": 4.0,
        },
        {
            "epoch": 2.0,
            "train_loss": 2.0,
            "val_loss": 1.5,
            "train_ml_vector_rmse": 1.2,
            "val_ml_vector_rmse": 0.8,
            "val_mass_vector_rmse": 4.0,
        },
    ])
    sample_rows = [
        {
            "sample_id": "a",
            "mass_vector_rmse": 4.0,
            "ml_vector_rmse": 1.0,
            "vector_rmse_improvement_percent": 75.0,
            "mass_speed_mae": 2.0,
            "ml_speed_mae": 0.5,
        },
        {
            "sample_id": "b",
            "mass_vector_rmse": 2.0,
            "ml_vector_rmse": 1.5,
            "vector_rmse_improvement_percent": 25.0,
            "mass_speed_mae": 1.0,
            "ml_speed_mae": 0.7,
        },
    ]
    samples = summarize_samples(sample_rows, top_n=1)
    distribution = metric_distribution(sample_rows, "ml_vector_rmse")

    assert training["epochs"] == 2
    assert training["best_epoch"] == 2
    assert samples["improved_sample_percent"] == 100.0
    assert samples["worst_ml_vector_rmse"][0]["sample_id"] == "b"
    assert distribution["min"] == 1.0
    assert distribution["max"] == 1.5


def test_controlled_profiles_and_custom_matrix():
    standard = profile_cases("standard")
    training = profile_cases("training")
    extreme_alias = profile_cases("extreme")
    custom = build_cases("standard", "3,7", "45,225")
    mph_custom = build_cases("standard", None, None, speeds_mph="10,80", direction_step=15)

    assert len(standard) == 60
    assert len(training) == 264
    assert training == extreme_alias
    assert max(case.speed_mps for case in training) == pytest.approx(mph_to_mps(80.0))
    assert len(custom) == 4
    assert custom[0].case_id == "s3mps_d045"
    assert len(mph_custom) == 48
    assert mph_custom[-1].case_id == "s35p763mps_d345"


def test_controlled_config_writes_mass_and_momentum_flags(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    case = build_cases("pilot", "5", "90")[0]
    runs = build_solver_runs([case], Path("runtime/ml/test_controlled"), ("mass", "momentum"))

    for run in runs:
        write_config(run, num_threads=4, output_height_m=10.0)

    mass_cfg = runs[0].config_host_path.read_text(encoding="utf-8")
    momentum_cfg = runs[1].config_host_path.read_text(encoding="utf-8")
    assert "mesh_resolution = 100.0" in mass_cfg
    assert "elevation_file = /opt/mountain_windninja/static_data/berthoud_pass.lcp" in mass_cfg
    assert "output_speed_units = mps" in mass_cfg
    assert "momentum_flag = false" in mass_cfg
    assert "momentum_flag = true" in momentum_cfg


def test_controlled_run_script_uses_direct_docker_commands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    case = build_cases("pilot", "5", "90")[0]
    runs = build_solver_runs([case], Path("runtime/ml/test_controlled"), ("mass",))
    script_path = tmp_path / "runtime" / "ml" / "test_controlled" / "run.sh"

    write_run_script(script_path, runs, num_threads=4)

    script = script_path.read_text(encoding="utf-8")
    container_script = script_path.with_name("run_container.sh").read_text(encoding="utf-8")
    assert "docker run --rm" in script
    assert "ghcr.io/austfi/mountain-windninja:3.12.2" in script
    assert "run_container.sh" in script
    assert "/opt/mountain_windninja/runtime/ml/test_controlled" in container_script
    assert "NINJAFOAM_mlr_controlled_berthoud_pass_s5mps_d090_mass" in container_script
    assert "NINJAFOAM_berthoud_pass_*" in container_script


def test_controlled_manifest_pairing_and_direction_split():
    rows = [
        {"case_id": "a", "solver": "mass"},
        {"case_id": "a", "solver": "momentum"},
        {"case_id": "b", "solver": "mass"},
    ]

    assert len(pair_manifest_rows(rows)) == 1
    assert controlled_split(60.0) == "test"
    assert controlled_split(150.0) == "test"
    assert controlled_split(30.0) == "val"
    assert controlled_split(120.0) == "val"
    assert controlled_split(90.0) == "train"


def test_controlled_dataset_metadata_prefers_domain_over_stale_terrain_path(tmp_path):
    rows = [
        {
            "case_id": "a",
            "solver": "mass",
            "domain_label": "keystone_9p6",
            "terrain_file": "/old/machine/static_data/keystone_9p6.lcp",
        },
        {
            "case_id": "a",
            "solver": "momentum",
            "domain_label": "keystone_9p6",
            "terrain_file": "/old/machine/static_data/keystone_9p6.lcp",
        },
    ]

    metadata = infer_controlled_metadata(rows, tmp_path / "controlled")

    assert metadata["source_dataset"] == "controlled_keystone_9p6"
    assert metadata["terrain_domain"] == "keystone_9p6"
    assert metadata["terrain_file"] == ""


def test_filter_rows_can_select_source_dataset():
    rows = [
        {"split": "test", "source_dataset": "berthoud_v0", "sample_id": "a"},
        {"split": "test", "source_dataset": "controlled_berthoud_training", "sample_id": "b"},
        {"split": "train", "source_dataset": "berthoud_v0", "sample_id": "c"},
    ]

    selected = filter_rows(rows, "test", source_dataset="controlled_berthoud_training")

    assert [row["sample_id"] for row in selected] == ["b"]


def test_build_combined_dataset_merges_sources_and_tracks_origin(tmp_path):
    hrrr_dir = tmp_path / "berthoud_v0"
    controlled_dir = tmp_path / "controlled_berthoud_training"
    _write_processed_sample(hrrr_dir, "hrrr_a", "train")
    _write_processed_sample(controlled_dir, "controlled_a", "train")

    summary = build_combined_dataset(
        hrrr_dir,
        controlled_dir,
        tmp_path / "combined",
    )
    manifest = (tmp_path / "combined" / "manifest.csv").read_text(encoding="utf-8")

    assert summary["sample_count"] == 2
    assert summary["split_counts"]["train"] == 2
    assert "berthoud_v0__hrrr_a" in manifest
    assert "controlled_berthoud_training__controlled_a" in manifest
    assert "source_dataset" in manifest


def test_build_combined_dataset_accepts_explicit_source_list(tmp_path):
    first = tmp_path / "first_source"
    second = tmp_path / "second_source"
    third = tmp_path / "third_source"
    _write_processed_sample(first, "a", "train")
    _write_processed_sample(second, "b", "val")
    _write_processed_sample(third, "c", "test")

    summary = build_combined_dataset(
        first,
        second,
        tmp_path / "combined_explicit",
        sources=[
            ("first_source", first),
            ("second_source", second),
            ("third_source", third),
        ],
    )
    manifest = (tmp_path / "combined_explicit" / "manifest.csv").read_text(encoding="utf-8")

    assert summary["sample_count"] == 3
    assert summary["source_datasets"]["third_source"]["sample_count"] == 1
    assert "third_source__c" in manifest


def test_lcp_canopy_channel_helpers_round_trip():
    assert input_channels_for_features(["canopy_cover"]) == LCP_CANOPY_CHANNELS
    assert terrain_features_from_input_channels(LCP_CANOPY_CHANNELS) == ["canopy_cover"]
    assert terrain_features_from_input_channels(CHANNELS) == []


def test_build_combined_dataset_preserves_lcp_canopy_channels(tmp_path):
    first = tmp_path / "first_lcp_source"
    second = tmp_path / "second_lcp_source"
    _write_processed_sample(first, "a", "train", input_channels=LCP_CANOPY_CHANNELS)
    _write_processed_sample(second, "b", "train", input_channels=LCP_CANOPY_CHANNELS)

    summary = build_combined_dataset(
        first,
        second,
        tmp_path / "combined_lcp",
        sources=[
            ("first_lcp_source", first),
            ("second_lcp_source", second),
        ],
    )

    assert summary["sample_count"] == 2
    assert summary["input_channels"] == LCP_CANOPY_CHANNELS
    normalization = (tmp_path / "combined_lcp" / "normalization.json").read_text(
        encoding="utf-8"
    )
    assert "canopy_cover" in normalization


def test_build_combined_dataset_rejects_mixed_channel_sources(tmp_path):
    first = tmp_path / "first_source"
    second = tmp_path / "second_source"
    _write_processed_sample(first, "a", "train")
    _write_processed_sample(second, "b", "train", input_channels=LCP_CANOPY_CHANNELS)

    with pytest.raises(ValueError, match="input channels"):
        build_combined_dataset(
            first,
            second,
            tmp_path / "combined_mixed_channels",
            sources=[
                ("first_source", first),
                ("second_source", second),
            ],
        )


def test_hrrr_pair_plan_builds_mass_and_momentum_runs():
    start = dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 4, 3, tzinfo=dt.timezone.utc)

    plan = plan_runs(
        start=start,
        end=end,
        chunk_hours=24,
        momentum_domain="berthoud_pass",
        mass_domain="berthoud_pass_mass",
        model="HRRR",
        threads=6,
    )

    assert plan["chunk_count"] == 2
    assert plan["run_count"] == 4
    assert plan["runs"][0]["solver"] == "mass"
    assert plan["runs"][0]["run_dir"] == (
        "runtime/temp/berthoud_pass_mass_20260401_0000_reanalysis_24h_HRRR"
    )
    assert plan["runs"][1]["solver"] == "momentum"
    assert plan["runs"][1]["run_dir"] == (
        "runtime/temp/berthoud_pass_20260401_0000_reanalysis_24h_HRRR"
    )


def test_hrrr_pair_plan_can_add_ml_inference_stage():
    start = dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 4, 2, tzinfo=dt.timezone.utc)

    plan = plan_runs(
        start=start,
        end=end,
        chunk_hours=24,
        momentum_domain="berthoud_pass",
        mass_domain="berthoud_pass_mass",
        model="HRRR",
        threads=6,
        label="large_emulator",
        infer_checkpoint="ml/residual_unet/colab/results/berthoud_combined_v1/best.pt",
    )

    assert plan["run_count"] == 2
    assert plan["inference_count"] == 1
    inference = plan["inferences"][0]
    assert inference["mass_run_dir"] == (
        "runtime/temp/berthoud_pass_mass_20260401_0000_reanalysis_24h_HRRR"
    )
    assert inference["momentum_run_dir"] == (
        "runtime/temp/berthoud_pass_20260401_0000_reanalysis_24h_HRRR"
    )
    assert inference["out_dir"] == (
        "runtime/ml/residual_unet/inference/hrrr_pairs/large_emulator/"
        "20260401_0000_reanalysis_24h_HRRR"
    )


def test_hrrr_pair_completion_counts_only_windninja_pairs(tmp_path):
    run_dir = tmp_path / run_dir_for(
        "berthoud_pass",
        dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        2,
        "HRRR",
    )
    _write_empty(run_dir / "berthoud_pass_04-01-2026_0000_100m_vel.asc")
    _write_empty(run_dir / "berthoud_pass_04-01-2026_0000_100m_ang.asc")
    _write_empty(run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-04-01-2026_0000_vel.asc")
    _write_empty(run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-04-01-2026_0000_ang.asc")

    run = {
        "hours": 1,
        "run_dir": run_dir.relative_to(tmp_path).as_posix(),
    }

    assert windninja_output_pair_count(run_dir) == 1
    assert is_complete_run(run, repo_root=tmp_path)


def test_inference_command_adds_momentum_run_when_complete(tmp_path):
    inference = {
        "checkpoint": "best.pt",
        "mass_run_dir": "runtime/temp/mass",
        "momentum_run_dir": "runtime/temp/momentum",
        "out_dir": "runtime/ml/inference",
        "hours": 1,
        "speed_units": "mph",
        "output_speed_units": "mph",
    }
    mom_dir = tmp_path / "runtime" / "temp" / "momentum"
    _write_empty(mom_dir / "berthoud_pass_04-01-2026_0000_100m_vel.asc")
    _write_empty(mom_dir / "berthoud_pass_04-01-2026_0000_100m_ang.asc")

    command = inference_command(inference, repo_root=tmp_path)

    assert command[1:3] == ["-m", "ml.residual_unet.infer"]
    assert "--momentum-run" in command


def test_inference_command_passes_terrain_options(tmp_path):
    inference = {
        "checkpoint": "best.pt",
        "mass_run_dir": "runtime/temp/mass",
        "momentum_run_dir": "runtime/temp/momentum",
        "out_dir": "runtime/ml/inference",
        "hours": 1,
        "speed_units": "mph",
        "output_speed_units": "mph",
        "terrain_file": "breck_tenmile_9p6.lcp",
        "terrain_domain": "breck_tenmile_9p6",
    }

    command = inference_command(inference, repo_root=tmp_path)

    assert command[1:3] == ["-m", "ml.residual_unet.infer"]
    assert command[command.index("--terrain-file") + 1] == "breck_tenmile_9p6.lcp"
    assert command[command.index("--terrain-domain") + 1] == "breck_tenmile_9p6"


def test_runtime_env_loads_prebuilt_image_for_compose(tmp_path):
    runtime_env_path = tmp_path / "config" / "runtime.env"
    runtime_env_path.parent.mkdir(parents=True)
    runtime_env_path.write_text(
        "MWN_DOCKER_IMAGE=ghcr.io/austfi/mountain-windninja:3.12.2\n",
        encoding="utf-8",
    )

    env = runtime_env(tmp_path)

    assert env["MWN_DOCKER_IMAGE"] == "ghcr.io/austfi/mountain-windninja:3.12.2"

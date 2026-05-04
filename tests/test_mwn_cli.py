from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def _copy_cli_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "deploy" / "gcp").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "config").mkdir()
    (repo / "static_data").mkdir()

    shutil.copy2(ROOT_DIR / "deploy" / "gcp" / "mwn.sh", repo / "deploy" / "gcp" / "mwn.sh")
    shutil.copy2(ROOT_DIR / "scripts" / "domain_registry.py", repo / "scripts" / "domain_registry.py")
    shutil.copy2(ROOT_DIR / "scripts" / "area_bounds.py", repo / "scripts" / "area_bounds.py")
    shutil.copy2(ROOT_DIR / "scripts" / "validation_plots.py", repo / "scripts" / "validation_plots.py")
    shutil.copy2(
        ROOT_DIR / "config" / "runtime.env.example",
        repo / "config" / "runtime.env.example",
    )
    (repo / "config" / "domains.json").write_text(
        json.dumps({
            "default_domain": "old",
            "domains": {
                "old": {
                    "label": "Old",
                    "template": "config/template.cfg",
                    "elevation_file": "old.tif",
                },
            },
        }),
        encoding="utf-8",
    )
    return repo


def _install_docker_stub(repo: Path) -> tuple[Path, dict[str, str]]:
    bin_dir = repo / "bin"
    bin_dir.mkdir()
    log_path = repo / "docker.log"
    docker = bin_dir / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$DOCKER_STUB_LOG"
if [ "${1:-}" = "info" ]; then
  exit 0
fi
if [ "${1:-}" = "compose" ] && [ "${2:-}" = "version" ]; then
  exit 0
fi
if [ "${1:-}" = "compose" ]; then
  exit 0
fi
echo "unexpected docker invocation: $*" >&2
exit 1
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["DOCKER_STUB_LOG"] = str(log_path)
    return log_path, env


def _run_cli(repo: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "deploy/gcp/mwn.sh", *args],
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_init_preserves_existing_runtime_env(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    runtime_env = repo / "config" / "runtime.env"
    runtime_env.write_text(
        "MWN_DOCKER_IMAGE=custom:image\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(repo, "init", "--image", "skip")

    assert result.returncode == 0, result.stderr + result.stdout
    assert runtime_env.read_text(encoding="utf-8") == (
        "MWN_DOCKER_IMAGE=custom:image\nMWN_DOMAIN_ID=old\n"
    )
    assert (repo / "runtime").is_dir()
    assert (repo / "static_data").is_dir()


def test_init_default_preserves_existing_docker_image(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    runtime_env = repo / "config" / "runtime.env"
    runtime_env.write_text(
        "MWN_DOCKER_IMAGE=custom:image\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(repo, "init", env=env)

    assert result.returncode == 0, result.stderr + result.stdout
    assert runtime_env.read_text(encoding="utf-8") == (
        "MWN_DOCKER_IMAGE=custom:image\nMWN_DOMAIN_ID=old\n"
    )
    docker_log = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    assert "pull" not in docker_log
    assert "Use --force-image" in result.stdout


def test_init_default_pull_falls_back_to_local_image_hint(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    _log_path, env = _install_docker_stub(repo)

    result = _run_cli(repo, "init", env=env)

    assert result.returncode == 0, result.stderr + result.stdout
    runtime_env = (repo / "config" / "runtime.env").read_text(encoding="utf-8")
    assert "MWN_DOCKER_IMAGE=mountain-windninja:local" in runtime_env
    assert "Fallback: ./deploy/gcp/mwn.sh build-local" in result.stdout


def test_build_local_records_local_image(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    _log_path, env = _install_docker_stub(repo)
    runtime_env = repo / "config" / "runtime.env"
    runtime_env.write_text(
        "MWN_DOCKER_IMAGE=ghcr.io/example/old:latest\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(repo, "build-local", env=env)

    assert result.returncode == 0, result.stderr + result.stdout
    assert "MWN_DOCKER_IMAGE=mountain-windninja:local" in runtime_env.read_text(
        encoding="utf-8"
    )


def test_help_is_beginner_focused(tmp_path):
    repo = _copy_cli_repo(tmp_path)

    result = _run_cli(repo, "help")

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Beginner path:" in result.stdout
    assert "fetch-terrain" in result.stdout
    assert "synoptic-points" not in result.stdout


def test_help_advanced_lists_advanced_commands(tmp_path):
    repo = _copy_cli_repo(tmp_path)

    result = _run_cli(repo, "help", "advanced")

    assert result.returncode == 0, result.stderr + result.stdout
    assert "advanced commands" in result.stdout
    assert "fetch-dem" in result.stdout
    assert "run-grid" in result.stdout
    assert "forcing-from-grib" in result.stdout
    assert "synoptic-points" in result.stdout
    assert "plot-validation" in result.stdout


def test_demo_smoke_restores_domain_config(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    original_domains = (repo / "config" / "domains.json").read_text(encoding="utf-8")
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(repo, "demo-smoke", "--keep-temp", env=env)

    assert result.returncode == 0, result.stderr + result.stdout
    assert (repo / "config" / "domains.json").read_text(encoding="utf-8") == original_domains
    assert not (repo / "static_data" / "demo_smoke.tif").exists()

    docker_log = log_path.read_text(encoding="utf-8")
    assert "gdal_create" in docker_log
    assert "preflight_check.py --domain demo_smoke" in docker_log
    assert "daily_run.py" in docker_log
    assert "--keep-temp" in docker_log


def test_run_grid_dispatches_to_gridded_script(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)

    result = _run_cli(
        repo,
        "run-grid",
        "--speed-grid",
        "runtime/forcing/case/speed.asc",
        "--direction-grid",
        "runtime/forcing/case/direction.asc",
        "--time",
        "202601010000",
        "--domain",
        "old",
        "--dry-run",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    docker_log = log_path.read_text(encoding="utf-8")
    assert "gridded_run.py" in docker_log
    assert "--speed-grid runtime/forcing/case/speed.asc" in docker_log
    assert "--direction-grid runtime/forcing/case/direction.asc" in docker_log
    assert "--dry-run" in docker_log


def test_forcing_from_grib_dispatches_to_converter_script(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)

    result = _run_cli(
        repo,
        "forcing-from-grib",
        "runtime/forcing/raw/input.grib2",
        "--domain",
        "old",
        "--time",
        "202601010000",
        "--u-var",
        "UGRD",
        "--v-var",
        "VGRD",
        "--level",
        "10m",
        "--out",
        "runtime/forcing/case",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    docker_log = log_path.read_text(encoding="utf-8")
    assert "forcing_from_grib.py" in docker_log
    assert "runtime/forcing/raw/input.grib2" in docker_log
    assert "--out runtime/forcing/case" in docker_log


def test_validate_study_dispatches_to_study_script(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)

    result = _run_cli(
        repo,
        "validate-study",
        "berthoud_pass",
        "--start",
        "202601010000",
        "--pilot-hours",
        "3",
        "--plan",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    docker_log = log_path.read_text(encoding="utf-8")
    assert "validation_study.py" in docker_log
    assert "berthoud_pass" in docker_log
    assert "--pilot-hours 3" in docker_log


def test_plot_validation_runs_on_host_python(tmp_path):
    repo = _copy_cli_repo(tmp_path)

    result = _run_cli(repo, "plot-validation", "--help")

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Create SVG/HTML plots" in result.stdout


def test_domain_create_registers_default_and_runs_check_with_default_output(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOCKER_IMAGE=mountain-windninja:local\n"
        "MWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "domain",
        "create",
        "smoke_test",
        "--bbox",
        "39.65",
        "-106.0",
        "39.55",
        "-106.15",
        "--terrain-source",
        "us",
        "--label",
        "Smoke Test",
        "--resolution",
        "30",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "smoke_test"
    assert payload["domains"]["smoke_test"] == {
        "label": "Smoke Test",
        "template": "config/template.cfg",
        "elevation_file": "smoke_test.tif",
    }
    assert "MWN_DOMAIN_ID=smoke_test" in (repo / "config" / "runtime.env").read_text(
        encoding="utf-8"
    )

    docker_log = log_path.read_text(encoding="utf-8")
    assert "gdalwarp" in docker_log
    assert "static_data/smoke_test.tif" in docker_log
    assert "preflight_check.py --domain smoke_test" in docker_log


def test_domain_create_no_default_no_check_lcp(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "domain",
        "create",
        "lcp_area",
        "--bbox",
        "39.65",
        "-106.0",
        "39.55",
        "-106.15",
        "--terrain-source",
        "lcp",
        "--no-set-default",
        "--no-check",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "old"
    assert payload["domains"]["lcp_area"]["elevation_file"] == "lcp_area.lcp"
    assert "MWN_DOMAIN_ID=old" in (repo / "config" / "runtime.env").read_text(
        encoding="utf-8"
    )

    docker_log = log_path.read_text(encoding="utf-8")
    assert "--src lcp" in docker_log
    assert "static_data/lcp_area.lcp" in docker_log
    assert "preflight_check.py --domain lcp_area" not in docker_log


def test_fetch_dem_domain_alias_registers_default_with_default_output(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-dem",
        "39.65",
        "-106.0",
        "39.55",
        "-106.15",
        "--domain",
        "keystone",
        "--label",
        "Keystone",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "keystone"
    assert payload["domains"]["keystone"] == {
        "label": "Keystone",
        "template": "config/template.cfg",
        "elevation_file": "keystone.tif",
    }
    assert "MWN_DOMAIN_ID=keystone" in (repo / "config" / "runtime.env").read_text(
        encoding="utf-8"
    )
    docker_log = log_path.read_text(encoding="utf-8")
    assert "gdalwarp" in docker_log
    assert "static_data/keystone.tif" in docker_log


def test_fetch_dem_center_size_registers_default_with_computed_bbox(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-dem",
        "--center",
        "39.60",
        "-106.08",
        "--size-km",
        "12",
        "--domain",
        "center_area",
        "--label",
        "Center Area",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Area: center 39.60,-106.08 | 12 km square" in result.stdout
    assert "BBox: north=" in result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "center_area"
    assert payload["domains"]["center_area"]["elevation_file"] == "center_area.tif"
    docker_log = log_path.read_text(encoding="utf-8")
    assert "gdalwarp" in docker_log
    assert "static_data/center_area.tif" in docker_log


def test_fetch_terrain_downloads_dem_and_lcp_then_registers_lcp_active(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-terrain",
        "--center",
        "39.60",
        "-106.08",
        "--size-km",
        "12",
        "--domain",
        "combined_area",
        "--label",
        "Combined Area",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Step 1/2: downloading DEM fallback" in result.stdout
    assert "Step 2/2: downloading LCP active terrain" in result.stdout
    assert "Active domain 'combined_area' uses LCP" in result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "combined_area"
    assert payload["domains"]["combined_area"] == {
        "label": "Combined Area",
        "template": "config/template.cfg",
        "elevation_file": "combined_area.lcp",
    }
    assert "MWN_DOMAIN_ID=combined_area" in (repo / "config" / "runtime.env").read_text(
        encoding="utf-8"
    )
    docker_log = log_path.read_text(encoding="utf-8")
    assert "gdalwarp" in docker_log
    assert "static_data/combined_area.tif" in docker_log
    assert "--src lcp" in docker_log
    assert "static_data/combined_area.lcp" in docker_log


def test_fetch_dem_register_domain_alias_does_not_set_default_by_default(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    _log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-dem",
        "39.65",
        "-106.0",
        "39.55",
        "-106.15",
        "--register-domain",
        "legacy",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "old"
    assert payload["domains"]["legacy"]["elevation_file"] == "legacy.tif"
    assert "MWN_DOMAIN_ID=old" in (repo / "config" / "runtime.env").read_text(
        encoding="utf-8"
    )


def test_fetch_lcp_domain_alias_registers_default_with_default_output(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-lcp",
        "39.65",
        "-106.0",
        "39.55",
        "-106.15",
        "--domain",
        "forest",
        "--label",
        "Forest",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "forest"
    assert payload["domains"]["forest"] == {
        "label": "Forest",
        "template": "config/template.cfg",
        "elevation_file": "forest.lcp",
    }
    docker_log = log_path.read_text(encoding="utf-8")
    assert "--src lcp" in docker_log
    assert "static_data/forest.lcp" in docker_log


def test_fetch_lcp_area_file_registers_default_with_computed_bbox(tmp_path):
    repo = _copy_cli_repo(tmp_path)
    log_path, env = _install_docker_stub(repo)
    (repo / "config" / "runtime.env").write_text(
        "MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n",
        encoding="utf-8",
    )
    (repo / "area.kml").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <Placemark>
      <Polygon>
        <outerBoundaryIs>
          <LinearRing>
            <coordinates>
              -106.15,39.55,0 -106.00,39.55,0 -106.00,39.65,0 -106.15,39.65,0
            </coordinates>
          </LinearRing>
        </outerBoundaryIs>
      </Polygon>
    </Placemark>
  </Document>
</kml>
""",
        encoding="utf-8",
    )

    result = _run_cli(
        repo,
        "fetch-lcp",
        "--area-file",
        "area.kml",
        "--padding-km",
        "1",
        "--domain",
        "kml_area",
        "--label",
        "KML Area",
        env=env,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Area: file area.kml | 1 km padding" in result.stdout
    assert "BBox: north=" in result.stdout
    payload = json.loads((repo / "config" / "domains.json").read_text(encoding="utf-8"))
    assert payload["default_domain"] == "kml_area"
    assert payload["domains"]["kml_area"]["elevation_file"] == "kml_area.lcp"
    docker_log = log_path.read_text(encoding="utf-8")
    assert "--src lcp" in docker_log
    assert "static_data/kml_area.lcp" in docker_log

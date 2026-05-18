"""Generate and optionally run controlled WindNinja mass/momentum pairs."""
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


STANDARD_RAW_ROOT = Path("runtime/ml/residual_unet/raw/controlled_berthoud")
DEFAULT_RAW_ROOT = Path("runtime/ml/residual_unet/raw/controlled_berthoud_training")
DEFAULT_DOCKER_IMAGE = "ghcr.io/austfi/mountain-windninja:3.12.2"
MPH_TO_MPS = 0.44704
CONTAINER_REPO = Path("/opt/mountain_windninja")
DEFAULT_DOMAIN_LABEL = "berthoud_pass"
DEFAULT_TERRAIN_FILE = Path("static_data/berthoud_pass.lcp")


@dataclass(frozen=True)
class ControlledCase:
    speed_mps: float
    direction_deg: float

    @property
    def speed_token(self) -> str:
        speed = f"{self.speed_mps:.3f}".rstrip("0").rstrip(".")
        return speed.replace(".", "p")

    @property
    def direction_token(self) -> str:
        return f"{int(round(self.direction_deg)) % 360:03d}"

    @property
    def case_id(self) -> str:
        return f"s{self.speed_token}mps_d{self.direction_token}"


@dataclass(frozen=True)
class SolverRun:
    case: ControlledCase
    solver: str
    domain_label: str
    terrain_host_path: Path
    terrain_container_path: Path
    config_host_path: Path
    config_container_path: Path
    output_host_dir: Path
    output_container_dir: Path

    @property
    def momentum_enabled(self) -> bool:
        return self.solver == "momentum"


def profile_cases(profile: str) -> list[ControlledCase]:
    if profile == "pilot":
        speeds = [5.0, 15.0]
        directions = [0.0, 90.0, 180.0, 270.0]
    elif profile == "standard":
        speeds = [2.0, 5.0, 10.0, 15.0, 20.0]
        directions = [float(value) for value in range(0, 360, 30)]
    elif profile == "dense":
        speeds = [2.0, 5.0, 8.0, 10.0, 12.0, 15.0, 20.0]
        directions = [float(value) for value in range(0, 360, 15)]
    elif profile in {"training", "extreme"}:
        speed_mph = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
        speeds = [mph_to_mps(value) for value in speed_mph]
        directions = [float(value) for value in range(0, 360, 15)]
    else:
        raise ValueError(f"Unsupported profile: {profile}")
    return [ControlledCase(speed, direction) for speed in speeds for direction in directions]


def safe_label(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in value)
    return safe.strip("_") or DEFAULT_DOMAIN_LABEL


def default_raw_root_for_profile(profile: str, domain_label: str = DEFAULT_DOMAIN_LABEL) -> Path:
    domain_token = safe_label(domain_label)
    if profile == "standard":
        root = STANDARD_RAW_ROOT
    elif profile in {"training", "extreme"}:
        root = DEFAULT_RAW_ROOT
    else:
        root = DEFAULT_RAW_ROOT.with_name(f"controlled_berthoud_{profile}")
    if domain_token == DEFAULT_DOMAIN_LABEL:
        return root
    return root.with_name(f"{root.name}_{domain_token}")


def mph_to_mps(speed_mph: float) -> float:
    return round(float(speed_mph) * MPH_TO_MPS, 4)


def mps_to_mph(speed_mps: float) -> float:
    return round(float(speed_mps) / MPH_TO_MPS, 3)


def parse_float_list(raw: str | None) -> list[float] | None:
    if raw is None:
        return None
    values = []
    for part in raw.split(","):
        stripped = part.strip()
        if stripped:
            values.append(float(stripped))
    return values


def direction_values_from_step(step: float) -> list[float]:
    if step <= 0 or step > 360:
        raise ValueError("--direction-step must be greater than 0 and no more than 360.")
    directions = []
    value = 0.0
    while value < 360.0 - 1e-9:
        directions.append(round(value, 6))
        value += step
    return directions


def build_cases(
    profile: str,
    speeds: str | None,
    directions: str | None,
    *,
    speeds_mph: str | None = None,
    direction_step: float | None = None,
) -> list[ControlledCase]:
    if speeds is not None and speeds_mph is not None:
        raise ValueError("Use either --speeds m/s or --speeds-mph, not both.")
    if directions is not None and direction_step is not None:
        raise ValueError("Use either --directions or --direction-step, not both.")
    explicit_speeds = parse_float_list(speeds)
    explicit_speeds_mph = parse_float_list(speeds_mph)
    explicit_directions = parse_float_list(directions)
    explicit_direction_step = direction_step
    if explicit_speeds is None and explicit_directions is None:
        if explicit_speeds_mph is None and explicit_direction_step is None:
            return profile_cases(profile)

    if explicit_speeds_mph is not None:
        explicit_speeds = [mph_to_mps(value) for value in explicit_speeds_mph]

    if explicit_direction_step is not None:
        explicit_directions = direction_values_from_step(explicit_direction_step)

    if explicit_speeds is None and explicit_directions is None:
        return profile_cases(profile)

    base_cases = profile_cases(profile)
    speed_values = explicit_speeds or sorted({case.speed_mps for case in base_cases})
    direction_values = explicit_directions or sorted({case.direction_deg for case in base_cases})
    return [
        ControlledCase(float(speed), float(direction) % 360.0)
        for speed in speed_values
        for direction in direction_values
    ]


def write_config(run: SolverRun, *, num_threads: int, output_height_m: float) -> None:
    run.output_host_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        f"num_threads = {num_threads}",
        f"elevation_file = {run.terrain_container_path.as_posix()}",
        "",
        "initialization_method = domainAverageInitialization",
        f"input_speed = {run.case.speed_mps}",
        "input_speed_units = mps",
        f"input_direction = {run.case.direction_deg}",
        "input_wind_height = 10.0",
        "units_input_wind_height = m",
        "",
        "diurnal_winds = false",
        "",
        "year = 2026",
        "month = 1",
        "day = 1",
        "hour = 0",
        "minute = 0",
        "time_zone = UTC",
        "",
        "mesh_resolution = 100.0",
        "units_mesh_resolution = m",
        "",
        f"momentum_flag = {'true' if run.momentum_enabled else 'false'}",
        "number_of_iterations = 300",
        "",
        f"output_wind_height = {output_height_m}",
        "units_output_wind_height = m",
        "output_speed_units = mps",
        "",
        "write_goog_output = false",
        "goog_out_use_consistent_color_scale = false",
        "units_goog_out_resolution = m",
        "",
        "write_ascii_output = true",
        "ascii_out_resolution = -1",
        "units_ascii_out_resolution = m",
        "",
        "write_shapefile_output = false",
        "write_wx_model_goog_output = false",
        "write_wx_model_ascii_output = false",
        "",
        f"output_path = {run.output_container_dir.as_posix()}",
        "",
    ]
    run.config_host_path.parent.mkdir(parents=True, exist_ok=True)
    run.config_host_path.write_text("\n".join(lines), encoding="utf-8")


def build_solver_runs(
    cases: list[ControlledCase],
    raw_root: Path,
    solvers: tuple[str, ...],
    *,
    domain_label: str = DEFAULT_DOMAIN_LABEL,
    terrain_file: Path = DEFAULT_TERRAIN_FILE,
) -> list[SolverRun]:
    runs = []
    repo_root = Path.cwd().resolve()
    domain_token = safe_label(domain_label)
    terrain_host_path = terrain_file.resolve()
    try:
        relative_terrain = terrain_host_path.relative_to(repo_root).as_posix()
    except ValueError as exc:
        raise ValueError(f"Controlled terrain file must be inside the repo: {terrain_file}") from exc
    terrain_container_path = CONTAINER_REPO / relative_terrain
    for case in cases:
        for solver in solvers:
            case_root = raw_root / case.case_id / solver
            config_name = f"mlr_controlled_{domain_token}_{case.case_id}_{solver}.cfg"
            config_host_path = case_root / config_name
            output_host_dir = case_root / "output"
            try:
                relative_config = config_host_path.resolve().relative_to(repo_root).as_posix()
                relative_output = output_host_dir.resolve().relative_to(repo_root).as_posix()
            except ValueError as exc:
                raise ValueError(
                    f"Controlled raw root must be inside the repo: {raw_root}"
                ) from exc
            runs.append(
                SolverRun(
                    case=case,
                    solver=solver,
                    domain_label=domain_label,
                    terrain_host_path=terrain_host_path,
                    terrain_container_path=terrain_container_path,
                    config_host_path=config_host_path,
                    config_container_path=CONTAINER_REPO / relative_config,
                    output_host_dir=output_host_dir,
                    output_container_dir=CONTAINER_REPO / relative_output,
                )
            )
    return runs


def write_manifest(path: Path, runs: list[SolverRun]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "case_id",
                "speed_mps",
                "speed_mph",
                "direction_deg",
                "domain_label",
                "terrain_file",
                "solver",
                "config_path",
                "output_dir",
            ],
        )
        writer.writeheader()
        for run in runs:
            writer.writerow({
                "case_id": run.case.case_id,
                "speed_mps": run.case.speed_mps,
                "speed_mph": mps_to_mph(run.case.speed_mps),
                "direction_deg": run.case.direction_deg,
                "domain_label": run.domain_label,
                "terrain_file": run.terrain_host_path.as_posix(),
                "solver": run.solver,
                "config_path": run.config_host_path.as_posix(),
                "output_dir": run.output_host_dir.as_posix(),
            })


def has_complete_ascii_output(output_dir: Path) -> bool:
    speed_paths = sorted(output_dir.glob("*_vel.asc"))
    for speed_path in speed_paths:
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if direction_path.exists():
            return True
    return False


def clean_case_cache(repo_root: Path, config_path: Path) -> None:
    config_basename = config_path.stem
    cache_dir = repo_root / "static_data" / f"NINJAFOAM_{config_basename}"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def runtime_docker_image(repo_root: Path) -> str:
    if image := os.environ.get("MWN_DOCKER_IMAGE"):
        return image
    env_path = repo_root / "config" / "runtime.env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("MWN_DOCKER_IMAGE="):
                return stripped.split("=", 1)[1].strip().strip("\"'")
    return DEFAULT_DOCKER_IMAGE


def run_windninja_config(run: SolverRun, *, num_threads: int | None = None) -> None:
    repo_root = Path.cwd().resolve()
    clean_case_cache(repo_root, run.config_host_path)
    run.output_host_dir.mkdir(parents=True, exist_ok=True)
    log_path = run.output_host_dir / "windninja.log"
    status_path = run.output_host_dir / "run_status.json"
    command = [
        "docker",
        "run",
        "--rm",
    ]
    env_path = repo_root / "config" / "runtime.env"
    if env_path.exists():
        command += ["--env-file", env_path.as_posix()]
    if num_threads is not None:
        command += ["-e", f"MWN_NUM_THREADS={num_threads}"]
    command += [
        "-v",
        f"{repo_root / 'config'}:/opt/mountain_windninja/config:ro",
        "-v",
        f"{repo_root / 'scripts'}:/opt/mountain_windninja/scripts:ro",
        "-v",
        f"{repo_root / 'docker'}:/opt/mountain_windninja/docker:ro",
        "-v",
        f"{repo_root / 'runtime'}:/opt/mountain_windninja/runtime",
        "-v",
        f"{repo_root / 'static_data'}:/opt/mountain_windninja/static_data",
        "-w",
        "/opt/mountain_windninja",
    ]
    shell_command = (
        "source /opt/openfoam9/etc/bashrc 2>/dev/null || true\n"
        "export FOAM_USER_LIBBIN=/usr/local/lib/\n"
        f"WindNinja_cli {shlex.quote(run.config_container_path.as_posix())}"
    )
    command += [runtime_docker_image(repo_root), "bash", "-lc", shell_command]
    result = subprocess.run(command, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    log_path.write_text(result.stdout or "", encoding="utf-8")
    status = {
        "case_id": run.case.case_id,
        "solver": run.solver,
        "returncode": result.returncode,
        "log_path": log_path.as_posix(),
    }
    status_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    if result.returncode != 0:
        tail = "\n".join((result.stdout or "").splitlines()[-40:])
        raise RuntimeError(
            f"WindNinja failed for {run.case.case_id} {run.solver}; "
            f"see {log_path}\n{tail}"
        )


def write_summary(
    path: Path,
    *,
    profile: str,
    cases: list[ControlledCase],
    runs: list[SolverRun],
    domain_label: str,
    terrain_file: Path,
) -> None:
    summary = {
        "profile": profile,
        "domain_label": domain_label,
        "terrain_file": terrain_file.as_posix(),
        "case_count": len(cases),
        "run_count": len(runs),
        "speeds_mps": sorted({case.speed_mps for case in cases}),
        "speeds_mph": sorted({mps_to_mph(case.speed_mps) for case in cases}),
        "directions_deg": sorted({case.direction_deg for case in cases}),
        "solvers": sorted({run.solver for run in runs}),
    }
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


def write_run_script(path: Path, runs: list[SolverRun], *, num_threads: int) -> None:
    """Write a single-container Docker runner for a controlled WindNinja matrix."""
    repo_root = Path.cwd().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    repo_root_from_script = os.path.relpath(repo_root, path.parent.resolve())
    container_script_path = path.with_name(f"{path.stem}_container.sh")
    container_script_container_path = (
        CONTAINER_REPO / container_script_path.resolve().relative_to(repo_root).as_posix()
    )

    host_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        f'REPO_ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/{repo_root_from_script}" && pwd)"',
        'cd "$REPO_ROOT"',
        "",
        f"NUM_THREADS={num_threads}",
        'SKIP_EXISTING="${SKIP_EXISTING:-1}"',
        'MAX_FAILURES="${MAX_FAILURES:-3}"',
        'CLEAN_MESH_CACHE="${CLEAN_MESH_CACHE:-0}"',
        'DOCKER_ENV_ARGS=()',
        'if [ -f config/runtime.env ]; then',
        '  set -a',
        '  # shellcheck disable=SC1091',
        '  . config/runtime.env',
        '  set +a',
        '  DOCKER_ENV_ARGS=(--env-file config/runtime.env)',
        "fi",
        'IMAGE="${MWN_DOCKER_IMAGE:-ghcr.io/austfi/mountain-windninja:3.12.2}"',
        "",
        'docker run --rm "${DOCKER_ENV_ARGS[@]}" \\',
        '  -e "MWN_NUM_THREADS=$NUM_THREADS" \\',
        '  -e "SKIP_EXISTING=$SKIP_EXISTING" \\',
        '  -e "MAX_FAILURES=$MAX_FAILURES" \\',
        '  -e "CLEAN_MESH_CACHE=$CLEAN_MESH_CACHE" \\',
        '  -v "$REPO_ROOT/config:/opt/mountain_windninja/config:ro" \\',
        '  -v "$REPO_ROOT/scripts:/opt/mountain_windninja/scripts:ro" \\',
        '  -v "$REPO_ROOT/docker:/opt/mountain_windninja/docker:ro" \\',
        '  -v "$REPO_ROOT/runtime:/opt/mountain_windninja/runtime" \\',
        '  -v "$REPO_ROOT/static_data:/opt/mountain_windninja/static_data" \\',
        '  -w /opt/mountain_windninja \\',
        f'  "$IMAGE" bash {shlex.quote(container_script_container_path.as_posix())}',
        "",
    ]

    container_lines = [
        "#!/usr/bin/env bash",
        "set -o pipefail",
        "",
        "REPO_ROOT=/opt/mountain_windninja",
        'cd "$REPO_ROOT"',
        "source /opt/openfoam9/etc/bashrc 2>/dev/null || true",
        "set -u",
        "export FOAM_USER_LIBBIN=/usr/local/lib/",
        'WINDNINJA_CLI="${MWN_WINDNINJA_CLI:-WindNinja_cli}"',
        'SKIP_EXISTING="${SKIP_EXISTING:-1}"',
        'MAX_FAILURES="${MAX_FAILURES:-3}"',
        'CLEAN_MESH_CACHE="${CLEAN_MESH_CACHE:-0}"',
        'failure_count=0',
        'run_count=0',
        "",
        "has_complete_output() {",
        '  local output_dir="$1"',
        '  local speed direction',
        '  for speed in "$output_dir"/*_vel.asc; do',
        '    [ -e "$speed" ] || continue',
        '    direction="${speed%_vel.asc}_ang.asc"',
        '    [ -e "$direction" ] && return 0',
        "  done",
        "  return 1",
        "}",
        "",
        "write_status() {",
        '  local status_path="$1"',
        '  local case_id="$2"',
        '  local solver="$3"',
        '  local returncode="$4"',
        '  local log_path="$5"',
        "  python3 - \"$status_path\" \"$case_id\" \"$solver\" \"$returncode\" \"$log_path\" <<'PY'",
        "import json",
        "import sys",
        "path, case_id, solver, returncode, log_path = sys.argv[1:]",
        "status = {",
        '    "case_id": case_id,',
        '    "solver": solver,',
        '    "returncode": int(returncode),',
        '    "log_path": log_path,',
        "}",
        "with open(path, 'w', encoding='utf-8') as f:",
        "    json.dump(status, f, indent=2)",
        "    f.write('\\n')",
        "PY",
        "}",
        "",
        "run_one() {",
        '  local case_id="$1"',
        '  local solver="$2"',
        '  local config_path="$3"',
        '  local output_rel="$4"',
        '  local cache_rel="$5"',
        '  local terrain_cache_glob="$6"',
        '  local output_dir="$REPO_ROOT/$output_rel"',
        '  local cache_dir="$REPO_ROOT/$cache_rel"',
        '  local log_path="$output_dir/windninja.log"',
        '  local status_path="$output_dir/run_status.json"',
        '  local status_log_path="$output_rel/windninja.log"',
        "  run_count=$((run_count + 1))",
        "",
        '  mkdir -p "$output_dir"',
        '  if [ "$SKIP_EXISTING" = "1" ] && has_complete_output "$output_dir"; then',
        '    echo "[$run_count] skip existing $case_id $solver"',
        '    return 0',
        "  fi",
        "",
        '  echo "[$run_count] run $case_id $solver"',
        '  if [ "$CLEAN_MESH_CACHE" = "1" ]; then',
        '    rm -rf "$cache_dir" "$REPO_ROOT"/$terrain_cache_glob',
        "  fi",
        '  "$WINDNINJA_CLI" "$config_path" >"$log_path" 2>&1',
        "  local returncode=$?",
        '  rm -f "$output_dir"/*_cld.asc "$output_dir"/*_cld.prj',
        '  write_status "$status_path" "$case_id" "$solver" "$returncode" "$status_log_path"',
        "  if [ \"$returncode\" -ne 0 ]; then",
        '    rm -rf "$cache_dir" "$REPO_ROOT"/$terrain_cache_glob',
        "    failure_count=$((failure_count + 1))",
        '    echo "FAILED $case_id $solver; see $log_path"',
        '    tail -40 "$log_path" || true',
        '    if [ "$failure_count" -ge "$MAX_FAILURES" ]; then',
        '      echo "Stopping after $failure_count failures."',
        "      exit 1",
        "    fi",
        "  fi",
        "}",
        "",
    ]
    for run in runs:
        cache_dir = Path("static_data") / f"NINJAFOAM_{run.config_host_path.stem}"
        terrain_cache_glob = Path("static_data") / f"NINJAFOAM_{run.terrain_host_path.stem}_*"
        output_rel = run.output_host_dir.resolve().relative_to(repo_root).as_posix()
        args = [
            run.case.case_id,
            run.solver,
            run.config_container_path.as_posix(),
            output_rel,
            cache_dir.as_posix(),
            terrain_cache_glob.as_posix(),
        ]
        quoted_args = " ".join(shlex.quote(arg) for arg in args)
        container_lines.append(f"run_one {quoted_args}")
    container_lines.extend([
        "",
        'echo "Finished $run_count planned runs with $failure_count failures."',
        'exit "$failure_count"',
        "",
    ])
    path.write_text("\n".join(host_lines), encoding="utf-8")
    container_script_path.write_text("\n".join(container_lines), encoding="utf-8")
    path.chmod(0o755)
    container_script_path.chmod(0o755)


def parse_solvers(raw: str) -> tuple[str, ...]:
    solvers = tuple(part.strip() for part in raw.split(",") if part.strip())
    allowed = {"mass", "momentum"}
    invalid = set(solvers) - allowed
    if invalid:
        raise ValueError(f"Unsupported solvers: {sorted(invalid)}")
    if not solvers:
        raise ValueError("At least one solver is required.")
    return solvers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and optionally run controlled WindNinja mass/momentum pairs."
    )
    parser.add_argument(
        "--profile",
        choices=["pilot", "standard", "dense", "training", "extreme"],
        default="training",
        help="Case matrix to generate. training uses 15-degree directions and speeds up to 80 mph.",
    )
    parser.add_argument("--speeds", help="Comma-separated m/s speeds overriding the profile.")
    parser.add_argument("--speeds-mph", help="Comma-separated mph speeds overriding the profile.")
    parser.add_argument("--directions", help="Comma-separated degrees overriding the profile.")
    parser.add_argument(
        "--direction-step",
        type=float,
        help="Direction spacing in degrees, for example 15 for 0,15,...,345.",
    )
    parser.add_argument("--raw-root", help="Output root. Defaults to a profile-specific runtime/ml path.")
    parser.add_argument("--domain-label", default=DEFAULT_DOMAIN_LABEL)
    parser.add_argument("--terrain-file", default=DEFAULT_TERRAIN_FILE.as_posix())
    parser.add_argument("--solvers", default="mass,momentum")
    parser.add_argument("--num-threads", type=int, default=4)
    parser.add_argument("--height", type=float, default=10.0)
    parser.add_argument("--plan", action="store_true", help="Print planned run counts and exit.")
    parser.add_argument("--write-configs", action="store_true", help="Write configs and manifest.")
    parser.add_argument(
        "--write-run-script",
        action="store_true",
        help="Write a direct-Docker shell runner beside the manifest.",
    )
    parser.add_argument(
        "--run-script-path",
        help="Optional path for --write-run-script. Defaults to raw-root/run_controlled_matrix.sh.",
    )
    parser.add_argument("--run", action="store_true", help="Run planned configs through Docker.")
    parser.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-runs", type=int, help="Optional cap for test execution.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    raw_root = (
        Path(args.raw_root)
        if args.raw_root
        else default_raw_root_for_profile(args.profile, args.domain_label)
    )
    terrain_file = Path(args.terrain_file)
    solvers = parse_solvers(args.solvers)
    cases = build_cases(
        args.profile,
        args.speeds,
        args.directions,
        speeds_mph=args.speeds_mph,
        direction_step=args.direction_step,
    )
    runs = build_solver_runs(
        cases,
        raw_root,
        solvers,
        domain_label=args.domain_label,
        terrain_file=terrain_file,
    )
    if args.max_runs is not None:
        runs = runs[:args.max_runs]

    print(json.dumps({
        "profile": args.profile,
        "raw_root": raw_root.as_posix(),
        "domain_label": args.domain_label,
        "terrain_file": terrain_file.as_posix(),
        "case_count": len(cases),
        "run_count": len(runs),
        "speeds_mps": sorted({case.speed_mps for case in cases}),
        "speeds_mph": sorted({mps_to_mph(case.speed_mps) for case in cases}),
        "directions_deg": sorted({case.direction_deg for case in cases}),
        "solvers": list(solvers),
    }, indent=2))

    if args.plan and not args.write_configs and not args.write_run_script and not args.run:
        return 0

    if not args.write_configs and not args.write_run_script and not args.run:
        raise SystemExit("Nothing to do. Use --plan, --write-configs, --write-run-script, or --run.")

    for run in runs:
        write_config(run, num_threads=args.num_threads, output_height_m=args.height)
    write_manifest(raw_root / "manifest.csv", runs)
    write_summary(
        raw_root / "controlled_summary.json",
        profile=args.profile,
        cases=cases,
        runs=runs,
        domain_label=args.domain_label,
        terrain_file=terrain_file,
    )
    if args.write_run_script:
        script_path = Path(args.run_script_path) if args.run_script_path else raw_root / "run_controlled_matrix.sh"
        write_run_script(script_path, runs, num_threads=args.num_threads)
        print(f"Wrote run script: {script_path}")

    if args.run:
        for index, run in enumerate(runs, start=1):
            if args.skip_existing and has_complete_ascii_output(run.output_host_dir):
                print(f"[{index}/{len(runs)}] skip existing {run.case.case_id} {run.solver}")
                continue
            print(f"[{index}/{len(runs)}] run {run.case.case_id} {run.solver}")
            run_windninja_config(run, num_threads=args.num_threads)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

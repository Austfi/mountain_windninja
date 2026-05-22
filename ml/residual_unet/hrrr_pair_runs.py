"""Plan and optionally launch paired HRRR mass/momentum WindNinja runs."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


UTC = dt.timezone.utc
DEFAULT_OUT_ROOT = Path("runtime/ml/residual_unet/hrrr_pairs")
DEFAULT_MOMENTUM_DOMAIN = "berthoud_pass"
DEFAULT_MASS_DOMAIN = "berthoud_pass_mass"
DEFAULT_MODEL = "HRRR"
DEFAULT_INFERENCE_OUT_ROOT = Path("runtime/ml/residual_unet/inference/hrrr_pairs")
DEFAULT_SPEED_UNITS = "mph"


@dataclass(frozen=True)
class Chunk:
    start: dt.datetime
    end: dt.datetime

    @property
    def hours(self) -> int:
        return int((self.end - self.start).total_seconds() / 3600)

    @property
    def label(self) -> str:
        return f"{self.start.strftime('%Y%m%d_%H%M')}_{self.end.strftime('%Y%m%d_%H%M')}"


def parse_utc(value: str) -> dt.datetime:
    raw = value.strip()
    for fmt in ("%Y%m%d%H%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            parsed = dt.datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ValueError(f"Expected UTC timestamp as YYYYMMDDHHMM: {value!r}")


def ymdhm(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def plan_chunks(start: dt.datetime, end: dt.datetime, chunk_hours: int) -> list[Chunk]:
    if start >= end:
        raise ValueError("--end must be later than --start.")
    for value, flag in ((start, "--start"), (end, "--end")):
        if value.minute or value.second or value.microsecond:
            raise ValueError(f"{flag} must be on an exact UTC hour boundary.")
    if chunk_hours < 1:
        raise ValueError("--chunk-hours must be >= 1.")

    chunks: list[Chunk] = []
    cursor = start
    delta = dt.timedelta(hours=chunk_hours)
    while cursor < end:
        chunk_end = min(cursor + delta, end)
        chunks.append(Chunk(cursor, chunk_end))
        cursor = chunk_end
    return chunks


def parse_window(value: str) -> tuple[dt.datetime, dt.datetime]:
    """Parse a repeatable START:END UTC window specification."""
    if ":" not in value:
        raise ValueError(f"Expected --window START:END, got {value!r}")
    raw_start, raw_end = value.split(":", 1)
    start = parse_utc(raw_start)
    end = parse_utc(raw_end)
    if start >= end:
        raise ValueError(f"Window end must be later than start: {value!r}")
    return start, end


def plan_chunks_for_windows(
    windows: list[tuple[dt.datetime, dt.datetime]],
    chunk_hours: int,
) -> list[Chunk]:
    chunks: list[Chunk] = []
    for start, end in sorted(windows):
        chunks.extend(plan_chunks(start, end, chunk_hours))
    return chunks


def run_dir_for(domain: str, start: dt.datetime, hours: int, model: str) -> Path:
    return Path("runtime/temp") / (
        f"{domain}_{start.strftime('%Y%m%d_%H%M')}_reanalysis_{hours}h_{model}"
    )


def inference_dir_for(label: str, start: dt.datetime, hours: int, model: str) -> Path:
    return DEFAULT_INFERENCE_OUT_ROOT / label / (
        f"{start.strftime('%Y%m%d_%H%M')}_reanalysis_{hours}h_{model}"
    )


def plan_runs(
    *,
    start: dt.datetime | None,
    end: dt.datetime | None,
    chunk_hours: int,
    momentum_domain: str,
    mass_domain: str,
    model: str,
    threads: int,
    label: str | None = None,
    infer_checkpoint: str | None = None,
    inference_out_root: str | Path = DEFAULT_INFERENCE_OUT_ROOT,
    speed_units: str = DEFAULT_SPEED_UNITS,
    output_speed_units: str = DEFAULT_SPEED_UNITS,
    terrain_file: str | None = None,
    terrain_domain: str | None = None,
    windows: list[tuple[dt.datetime, dt.datetime]] | None = None,
) -> dict:
    if windows:
        chunks = plan_chunks_for_windows(windows, chunk_hours)
        plan_start = min(window_start for window_start, _ in windows)
        plan_end = max(window_end for _, window_end in windows)
    else:
        if start is None or end is None:
            raise ValueError("start and end are required when windows are not provided.")
        chunks = plan_chunks(start, end, chunk_hours)
        plan_start = start
        plan_end = end
    runs = []
    inferences = []
    plan_label = label or f"{ymdhm(plan_start)}_{ymdhm(plan_end)}"
    for chunk in chunks:
        for solver, domain in (("mass", mass_domain), ("momentum", momentum_domain)):
            runs.append({
                "solver": solver,
                "domain": domain,
                "model": model,
                "start": ymdhm(chunk.start),
                "end": ymdhm(chunk.end),
                "hours": chunk.hours,
                "run_dir": run_dir_for(domain, chunk.start, chunk.hours, model).as_posix(),
            })
        if infer_checkpoint:
            inferences.append({
                "start": ymdhm(chunk.start),
                "end": ymdhm(chunk.end),
                "hours": chunk.hours,
                "model": model,
                "checkpoint": infer_checkpoint,
                "mass_run_dir": run_dir_for(mass_domain, chunk.start, chunk.hours, model).as_posix(),
                "momentum_run_dir": run_dir_for(momentum_domain, chunk.start, chunk.hours, model).as_posix(),
                "out_dir": (
                    Path(inference_out_root) / plan_label / (
                        f"{chunk.start.strftime('%Y%m%d_%H%M')}_reanalysis_{chunk.hours}h_{model}"
                    )
                ).as_posix(),
                "speed_units": speed_units,
                "output_speed_units": output_speed_units,
                "terrain_file": terrain_file or "",
                "terrain_domain": terrain_domain or momentum_domain,
            })
    return {
        "created_at_utc": dt.datetime.now(UTC).isoformat(),
        "start": ymdhm(plan_start),
        "end": ymdhm(plan_end),
        "label": plan_label,
        "chunk_hours": chunk_hours,
        "chunk_count": len(chunks),
        "run_count": len(runs),
        "threads": threads,
        "momentum_domain": momentum_domain,
        "mass_domain": mass_domain,
        "model": model,
        "runs": runs,
        "inference_count": len(inferences),
        "inferences": inferences,
        "terrain_file": terrain_file or "",
        "terrain_domain": terrain_domain or momentum_domain,
        "windows": [
            {"start": ymdhm(window_start), "end": ymdhm(window_end)}
            for window_start, window_end in (windows or [(plan_start, plan_end)])
        ],
    }


def windninja_output_pair_count(run_dir: Path) -> int:
    count = 0
    for speed_path in run_dir.glob("*_vel.asc"):
        if speed_path.name.startswith(("NOMADS-", "PASTCAST-")):
            continue
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if direction_path.exists():
            count += 1
    return count


def is_complete_run(run: dict, *, repo_root: Path) -> bool:
    run_dir = repo_root / run["run_dir"]
    return run_dir.exists() and windninja_output_pair_count(run_dir) >= int(run["hours"])


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{index:03d}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not find unused archive path for {path}")


def quarantine_incomplete_run_dir(run: dict, *, repo_root: Path, reason: str) -> None:
    run_dir = repo_root / run["run_dir"]
    if not run_dir.exists() or is_complete_run(run, repo_root=repo_root):
        return
    stamp = dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    archive_root = repo_root / "runtime/ml/residual_unet/failed_runs" / stamp
    target = unique_path(archive_root / run_dir.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"quarantine incomplete {run['solver']} {run['start']} reason={reason}: {run_dir} -> {target}")
    try:
        run_dir.rename(target)
    except PermissionError:
        subprocess.run(["sudo", "mv", str(run_dir), str(target)], check=True)


def clean_domain_mesh_cache(run: dict, *, repo_root: Path) -> None:
    static_root = repo_root / "static_data"
    domain = run["domain"]
    removed = 0
    for path in static_root.glob(f"NINJAFOAM_{domain}_*"):
        if path.is_dir():
            print(f"remove mesh cache after failed {run['solver']} {run['start']}: {path}")
            try:
                shutil.rmtree(path)
            except PermissionError:
                subprocess.run(["sudo", "rm", "-rf", str(path)], check=True)
            removed += 1
    if removed == 0:
        print(f"no mesh cache found for failed {run['solver']} {run['start']} domain={domain}")


def runtime_env(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    runtime_env_path = repo_root / "config" / "runtime.env"
    if not runtime_env_path.exists():
        return env
    for raw_line in runtime_env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return env


def is_complete_inference(inference: dict, *, repo_root: Path) -> bool:
    return (repo_root / inference["out_dir"] / "metadata.json").exists()


def daily_run_command(run: dict, *, threads: int) -> list[str]:
    return [
        "docker",
        "compose",
        "--profile",
        "tools",
        "run",
        "--rm",
        "-e",
        f"MWN_NUM_THREADS={threads}",
        "shell",
        "bash",
        "-lc",
        (
            "source /opt/openfoam9/etc/bashrc 2>/dev/null || true\n"
            "export FOAM_USER_LIBBIN=/usr/local/lib/\n"
            "cd /opt/mountain_windninja/runtime\n"
            "exec /opt/venv/bin/python /opt/mountain_windninja/scripts/daily_run.py \"$@\""
        ),
        "bash",
        "--mode",
        "reanalysis",
        "--start",
        run["start"],
        "--end",
        run["end"],
        "--model",
        run["model"],
        "--domain",
        run["domain"],
        "--keep-temp",
        "--no-upload",
    ]


def inference_command(inference: dict, *, repo_root: Path) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "ml.residual_unet.infer",
        "--checkpoint",
        inference["checkpoint"],
        "--mass-run",
        inference["mass_run_dir"],
        "--out",
        inference["out_dir"],
        "--source-root",
        ".",
        "--speed-units",
        inference.get("speed_units", DEFAULT_SPEED_UNITS),
        "--output-speed-units",
        inference.get("output_speed_units", inference.get("speed_units", DEFAULT_SPEED_UNITS)),
    ]
    if inference.get("terrain_file"):
        command.extend(["--terrain-file", inference["terrain_file"]])
    if inference.get("terrain_domain"):
        command.extend(["--terrain-domain", inference["terrain_domain"]])
    momentum_run_dir = repo_root / inference["momentum_run_dir"]
    if momentum_run_dir.exists() and windninja_output_pair_count(momentum_run_dir) >= int(inference["hours"]):
        command.extend(["--momentum-run", inference["momentum_run_dir"]])
    return command


def run_solver_step(
    run: dict,
    *,
    plan: dict,
    repo_root: Path,
    force: bool,
    skip_existing: bool,
    index_label: str,
) -> int:
    if skip_existing and not force and is_complete_run(run, repo_root=repo_root):
        print(f"[{index_label}] skip existing {run['solver']} {run['start']} {run['run_dir']}")
        return 0

    if not force:
        quarantine_incomplete_run_dir(run, repo_root=repo_root, reason="pre_run_incomplete")

    max_attempts = 2
    last_returncode = 0
    for attempt in range(1, max_attempts + 1):
        retry_label = "" if attempt == 1 else f" retry={attempt - 1}"
        print(f"[{index_label}] run {run['solver']} {run['start']} threads={plan['threads']}{retry_label}")
        result = subprocess.run(
            daily_run_command(run, threads=int(plan["threads"])),
            check=False,
            env=runtime_env(repo_root),
        )
        last_returncode = result.returncode
        if result.returncode == 0:
            return 0

        print(f"FAILED: {run['solver']} {run['start']} returncode={result.returncode}", file=sys.stderr)
        quarantine_incomplete_run_dir(run, repo_root=repo_root, reason="failed_solver")
        clean_domain_mesh_cache(run, repo_root=repo_root)
        if attempt < max_attempts:
            print(f"retry after cleanup: {run['solver']} {run['start']}")

    return last_returncode


def run_inference_step(
    inference: dict,
    *,
    repo_root: Path,
    force: bool,
    skip_existing: bool,
    index_label: str,
) -> int:
    if skip_existing and not force and is_complete_inference(inference, repo_root=repo_root):
        print(f"[{index_label}] skip existing {inference['start']} {inference['out_dir']}")
        return 0

    mass_run_dir = repo_root / inference["mass_run_dir"]
    if not mass_run_dir.exists() or windninja_output_pair_count(mass_run_dir) == 0:
        print(f"FAILED: missing mass run for ML inference {inference['start']}", file=sys.stderr)
        return 1

    print(f"[{index_label}] infer {inference['start']}")
    result = subprocess.run(inference_command(inference, repo_root=repo_root), check=False)
    if result.returncode != 0:
        print(f"FAILED: ML inference {inference['start']} returncode={result.returncode}", file=sys.stderr)
    return result.returncode


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_run_script(path: Path, plan_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join([
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"',
            'cd "$REPO_ROOT"',
            'PYTHON_BIN="python3"',
            'if [[ -x ".venv/bin/python" ]]; then',
            '  PYTHON_BIN=".venv/bin/python"',
            "fi",
            (
                f'"$PYTHON_BIN" -m ml.residual_unet.hrrr_pair_runs '
                f'--run-plan "{plan_path.as_posix()}" '
                '--max-failures "${MWN_ML_MAX_FAILURES:-100000}" '
                '--max-consecutive-failures "${MWN_ML_MAX_CONSECUTIVE_FAILURES:-3}" "$@"'
            ),
            "",
        ]),
        encoding="utf-8",
    )
    path.chmod(0o755)


def run_plan(
    plan_path: Path,
    *,
    force: bool,
    skip_existing: bool,
    max_failures: int,
    max_consecutive_failures: int,
) -> int:
    repo_root = Path.cwd().resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    failures = 0
    consecutive_failures = 0
    runs = plan["runs"]
    inferences = plan.get("inferences", [])
    if not inferences:
        for index, run in enumerate(runs, start=1):
            returncode = run_solver_step(
                run,
                plan=plan,
                repo_root=repo_root,
                force=force,
                skip_existing=skip_existing,
                index_label=f"{index}/{plan['run_count']}",
            )
            if returncode == 0:
                consecutive_failures = 0
                continue
            failures += 1
            consecutive_failures += 1
            if failures >= max_failures or consecutive_failures >= max_consecutive_failures:
                return returncode
        return 1 if failures else 0

    runs_by_chunk: dict[tuple[str, str], list[dict]] = {}
    for run in runs:
        runs_by_chunk.setdefault((run["start"], run["end"]), []).append(run)

    total_steps = len(runs) + len(inferences)
    step = 0
    for inference in inferences:
        chunk_runs = runs_by_chunk.get((inference["start"], inference["end"]), [])
        chunk_failed = False
        for run in chunk_runs:
            step += 1
            returncode = run_solver_step(
                run,
                plan=plan,
                repo_root=repo_root,
                force=force,
                skip_existing=skip_existing,
                index_label=f"{step}/{total_steps}",
            )
            if returncode == 0:
                consecutive_failures = 0
                continue
            chunk_failed = True
            failures += 1
            consecutive_failures += 1
            if failures >= max_failures or consecutive_failures >= max_consecutive_failures:
                return returncode
        step += 1
        if chunk_failed:
            print(f"[{step}/{total_steps} ml] skip inference after solver failure {inference['start']}")
            continue
        returncode = run_inference_step(
            inference,
            repo_root=repo_root,
            force=force,
            skip_existing=skip_existing,
            index_label=f"{step}/{total_steps} ml",
        )
        if returncode == 0:
            consecutive_failures = 0
            continue
        failures += 1
        consecutive_failures += 1
        if failures >= max_failures or consecutive_failures >= max_consecutive_failures:
            return returncode
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan paired HRRR WindNinja mass/momentum runs for ML training."
    )
    parser.add_argument("--start", help="UTC start, YYYYMMDDHHMM.")
    parser.add_argument("--end", help="UTC end, YYYYMMDDHHMM.")
    parser.add_argument(
        "--window",
        action="append",
        default=[],
        help=(
            "Repeatable UTC window as START:END, using YYYYMMDDHHMM timestamps. "
            "When provided, --start/--end are not required."
        ),
    )
    parser.add_argument("--chunk-hours", type=int, default=24)
    parser.add_argument("--momentum-domain", default=DEFAULT_MOMENTUM_DOMAIN)
    parser.add_argument("--mass-domain", default=DEFAULT_MASS_DOMAIN)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT.as_posix())
    parser.add_argument("--label", help="Plan folder label. Defaults to <start>_<end>.")
    parser.add_argument("--infer-checkpoint", help="Optional residual U-Net checkpoint for post-run ML inference.")
    parser.add_argument("--inference-out-root", default=DEFAULT_INFERENCE_OUT_ROOT.as_posix())
    parser.add_argument("--speed-units", default=DEFAULT_SPEED_UNITS)
    parser.add_argument("--output-speed-units", default=DEFAULT_SPEED_UNITS)
    parser.add_argument("--terrain-file", help="Optional terrain file for post-run ML inference.")
    parser.add_argument("--terrain-domain", help="Domain key used to find inference terrain.")
    parser.add_argument("--write-run-script", action="store_true")
    parser.add_argument("--run-plan", help="Run a previously written plan JSON.")
    parser.add_argument("--force", action="store_true", help="Run even when output appears complete.")
    parser.add_argument("--no-skip-existing", action="store_true")
    parser.add_argument("--max-failures", type=int, default=3)
    parser.add_argument("--max-consecutive-failures", type=int, default=3)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.run_plan:
        return run_plan(
            Path(args.run_plan),
            force=args.force,
            skip_existing=not args.no_skip_existing,
            max_failures=args.max_failures,
            max_consecutive_failures=args.max_consecutive_failures,
        )

    windows = [parse_window(value) for value in args.window]
    if windows:
        start = None
        end = None
        first_start = min(window_start for window_start, _ in windows)
        last_end = max(window_end for _, window_end in windows)
        label = args.label or f"{ymdhm(first_start)}_{ymdhm(last_end)}"
    else:
        if not args.start or not args.end:
            raise SystemExit("--start and --end, or at least one --window, are required unless --run-plan is used.")
        start = parse_utc(args.start)
        end = parse_utc(args.end)
        label = args.label or f"{ymdhm(start)}_{ymdhm(end)}"
    out_dir = Path(args.out_root) / label
    plan = plan_runs(
        start=start,
        end=end,
        chunk_hours=args.chunk_hours,
        momentum_domain=args.momentum_domain,
        mass_domain=args.mass_domain,
        model=args.model,
        threads=args.threads,
        label=label,
        infer_checkpoint=args.infer_checkpoint,
        inference_out_root=args.inference_out_root,
        speed_units=args.speed_units,
        output_speed_units=args.output_speed_units,
        terrain_file=args.terrain_file,
        terrain_domain=args.terrain_domain,
        windows=windows or None,
    )
    plan_path = out_dir / "plan.json"
    write_json(plan_path, plan)
    print(json.dumps({
        "plan_path": plan_path.as_posix(),
        "chunk_count": plan["chunk_count"],
        "run_count": plan["run_count"],
        "inference_count": plan["inference_count"],
        "threads": plan["threads"],
        "start": plan["start"],
        "end": plan["end"],
    }, indent=2))
    if args.write_run_script:
        script_path = out_dir / "run_hrrr_pairs.sh"
        write_run_script(script_path, plan_path)
        print(f"Wrote run script: {script_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

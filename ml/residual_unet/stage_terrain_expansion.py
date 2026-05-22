"""Stage terrain-expansion data generation plans for residual U-Net training."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from . import controlled_pairs
from .hrrr_pair_runs import parse_utc, plan_runs, write_json, write_run_script


UTC = dt.timezone.utc
DEFAULT_STAGE_LABEL = "terrain_expansion_wave1_v1"
DEFAULT_OUT_ROOT = Path("runtime/ml/residual_unet/terrain_expansion")
DEFAULT_HRRR_OUT_ROOT = Path("runtime/ml/residual_unet/hrrr_pairs")
DEFAULT_CONTROLLED_ROOT = Path("runtime/ml/residual_unet/raw/controlled_9p6_15deg")
DEFAULT_MONTHLY_WINDOW_STARTS = (
    "202505010000",
    "202505150000",
    "202506080000",
    "202506220000",
    "202507010000",
    "202507150000",
    "202508080000",
    "202508220000",
    "202509010000",
    "202509150000",
    "202510080000",
    "202510220000",
    "202511010000",
    "202511150000",
    "202512080000",
    "202512220000",
    "202601010000",
    "202601150000",
    "202602080000",
    "202602220000",
    "202603010000",
    "202603150000",
    "202604080000",
    "202604220000",
)


@dataclass(frozen=True)
class TerrainExpansionSpec:
    domain: str
    mass_domain: str
    label: str
    center_lat: float
    center_lon: float
    size_km: float = 9.6

    @property
    def terrain_file(self) -> Path:
        return Path("static_data") / f"{self.domain}.lcp"

    @property
    def hrrr_label(self) -> str:
        return f"{self.domain}_hrrr_lcp_canopy_v1"

    @property
    def smoke_label(self) -> str:
        return f"{self.domain}_smoke"


TERRAIN_EXPANSION_SPECS: tuple[TerrainExpansionSpec, ...] = (
    TerrainExpansionSpec(
        domain="copper_mountain_9p6",
        mass_domain="copper_mountain_9p6_mass",
        label="Copper Mountain 9.6 km",
        center_lat=39.4840,
        center_lon=-106.1516,
    ),
    TerrainExpansionSpec(
        domain="vail_central_9p6",
        mass_domain="vail_central_9p6_mass",
        label="Vail Central/Back Bowls 9.6 km",
        center_lat=39.6060,
        center_lon=-106.3740,
    ),
    TerrainExpansionSpec(
        domain="monarch_pass_9p6",
        mass_domain="monarch_pass_9p6_mass",
        label="Monarch Pass 9.6 km",
        center_lat=38.5103,
        center_lon=-106.3395,
    ),
)


def specs_by_domain() -> dict[str, TerrainExpansionSpec]:
    return {spec.domain: spec for spec in TERRAIN_EXPANSION_SPECS}


def select_specs(domains: Sequence[str] | None) -> list[TerrainExpansionSpec]:
    if not domains:
        return list(TERRAIN_EXPANSION_SPECS)
    available = specs_by_domain()
    selected = []
    for domain in domains:
        if domain not in available:
            raise ValueError(f"Unknown terrain-expansion domain: {domain}")
        selected.append(available[domain])
    return selected


def monthly_week_windows(
    starts: Sequence[str] = DEFAULT_MONTHLY_WINDOW_STARTS,
    *,
    days: int = 7,
) -> list[tuple[dt.datetime, dt.datetime]]:
    if days < 1:
        raise ValueError("Monthly window days must be >= 1.")
    windows = []
    for raw_start in starts:
        start = parse_utc(raw_start)
        end = start + dt.timedelta(days=days)
        windows.append((start, end))
    return windows


def repo_relative(path: Path, *, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def write_fetch_script(path: Path, specs: Sequence[TerrainExpansionSpec], *, repo_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    repo_root_from_script = os.path.relpath(repo_root.resolve(), path.parent.resolve())
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f'REPO_ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/{repo_root_from_script}" && pwd)"',
        'cd "$REPO_ROOT"',
        "",
        "# Downloads DEM fallback plus active LANDFIRE LCP terrain for each ML domain.",
    ]
    for spec in specs:
        lines.extend([
            "",
            f'echo "Fetching terrain: {spec.domain}"',
            "./deploy/gcp/mwn.sh fetch-terrain \\",
            f"  --center {spec.center_lat:.4f} {spec.center_lon:.4f} \\",
            f"  --size-km {spec.size_km:.1f} \\",
            f"  --domain {spec.domain} \\",
            f'  --label "{spec.label}"',
        ])
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def write_runner(path: Path, script_paths: Sequence[Path], *, repo_root: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    repo_root_from_script = os.path.relpath(repo_root.resolve(), path.parent.resolve())
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f'REPO_ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/{repo_root_from_script}" && pwd)"',
        'cd "$REPO_ROOT"',
        "",
        f'echo "{title}"',
    ]
    for script_path in script_paths:
        rel_path = repo_relative(script_path, repo_root=repo_root)
        lines.extend([
            "",
            f'echo "Running {rel_path}"',
            f'bash "{rel_path}"',
        ])
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def write_parallel_runner(
    path: Path,
    script_paths: Sequence[Path],
    *,
    repo_root: Path,
    title: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    repo_root_from_script = os.path.relpath(repo_root.resolve(), path.parent.resolve())
    log_dir = path.parent / "logs" / path.stem
    log_dir_rel = repo_relative(log_dir, repo_root=repo_root)
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f'REPO_ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/{repo_root_from_script}" && pwd)"',
        'cd "$REPO_ROOT"',
        "",
        f'echo "{title}"',
        f'mkdir -p "{log_dir_rel}"',
        "pids=()",
        "labels=()",
        "",
        "run_background() {",
        '  local label="$1"',
        '  local script_path="$2"',
        '  local log_path="$3"',
        '  echo "Starting $label -> $log_path"',
        '  bash "$script_path" >"$log_path" 2>&1 &',
        '  pids+=("$!")',
        '  labels+=("$label")',
        "}",
        "",
    ]
    for script_path in script_paths:
        rel_path = repo_relative(script_path, repo_root=repo_root)
        label = script_path.parent.name
        log_path = f"{log_dir_rel}/{label}.log"
        lines.append(f'run_background "{label}" "{rel_path}" "{log_path}"')
    lines.extend([
        "",
        "failures=0",
        'for index in "${!pids[@]}"; do',
        '  pid="${pids[$index]}"',
        '  label="${labels[$index]}"',
        '  if wait "$pid"; then',
        '    echo "Finished $label"',
        "  else",
        '    echo "FAILED $label"',
        "    failures=$((failures + 1))",
        "  fi",
        "done",
        "",
        'if [ "$failures" -ne 0 ]; then',
        '  echo "$failures parallel job(s) failed."',
        "  exit 1",
        "fi",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def stage_hrrr_plan(
    spec: TerrainExpansionSpec,
    *,
    label: str,
    windows: Sequence[tuple[dt.datetime, dt.datetime]],
    chunk_hours: int,
    threads: int,
    hrrr_out_root: Path,
) -> tuple[Path, dict]:
    plan = plan_runs(
        start=None,
        end=None,
        chunk_hours=chunk_hours,
        momentum_domain=spec.domain,
        mass_domain=spec.mass_domain,
        model="HRRR",
        threads=threads,
        label=label,
        terrain_domain=spec.domain,
        windows=list(windows),
    )
    plan_dir = hrrr_out_root / label
    plan_path = plan_dir / "plan.json"
    write_json(plan_path, plan)
    script_path = plan_dir / "run_hrrr_pairs.sh"
    write_run_script(script_path, plan_path)
    return script_path, plan


def stage_controlled_plan(
    spec: TerrainExpansionSpec,
    *,
    controlled_root: Path,
    profile: str,
    threads: int,
) -> tuple[Path, dict]:
    raw_root = controlled_root / spec.domain
    cases = controlled_pairs.build_cases(
        profile,
        speeds=None,
        directions=None,
    )
    runs = controlled_pairs.build_solver_runs(
        cases,
        raw_root,
        ("mass", "momentum"),
        domain_label=spec.domain,
        terrain_file=spec.terrain_file,
    )
    for run in runs:
        controlled_pairs.write_config(run, num_threads=threads, output_height_m=10.0)
    controlled_pairs.write_manifest(raw_root / "manifest.csv", runs)
    controlled_pairs.write_summary(
        raw_root / "controlled_summary.json",
        profile=profile,
        cases=cases,
        runs=runs,
        domain_label=spec.domain,
        terrain_file=spec.terrain_file,
    )
    script_path = raw_root / "run_controlled_matrix.sh"
    controlled_pairs.write_run_script(script_path, runs, num_threads=threads)
    summary = {
        "raw_root": raw_root.as_posix(),
        "profile": profile,
        "case_count": len(cases),
        "run_count": len(runs),
        "script_path": script_path.as_posix(),
    }
    return script_path, summary


def stage_terrain_expansion(
    *,
    domains: Sequence[str] | None = None,
    out_root: Path = DEFAULT_OUT_ROOT,
    label: str = DEFAULT_STAGE_LABEL,
    hrrr_out_root: Path = DEFAULT_HRRR_OUT_ROOT,
    controlled_root: Path = DEFAULT_CONTROLLED_ROOT,
    smoke_start: str = "202601010000",
    smoke_end: str = "202601020000",
    monthly_days: int = 7,
    chunk_hours: int = 24,
    threads: int = 4,
    controlled_profile: str = "training",
    write_fetch: bool = True,
    write_smoke: bool = True,
    write_monthly: bool = True,
    write_controlled: bool = True,
    repo_root: Path | None = None,
) -> dict:
    repo_root = (repo_root or Path.cwd()).resolve()
    specs = select_specs(domains)
    stage_dir = out_root / label
    stage_dir.mkdir(parents=True, exist_ok=True)

    fetch_script = None
    smoke_scripts = []
    monthly_scripts = []
    controlled_scripts = []
    domain_summaries = []

    if write_fetch:
        fetch_script = stage_dir / "fetch_terrain.sh"
        write_fetch_script(fetch_script, specs, repo_root=repo_root)

    smoke_windows = [(parse_utc(smoke_start), parse_utc(smoke_end))]
    monthly_windows = monthly_week_windows(days=monthly_days)
    for spec in specs:
        domain_summary = {"spec": asdict(spec)}
        if write_smoke:
            script_path, plan = stage_hrrr_plan(
                spec,
                label=spec.smoke_label,
                windows=smoke_windows,
                chunk_hours=chunk_hours,
                threads=threads,
                hrrr_out_root=hrrr_out_root,
            )
            smoke_scripts.append(script_path)
            domain_summary["smoke"] = {
                "plan_path": (hrrr_out_root / spec.smoke_label / "plan.json").as_posix(),
                "script_path": script_path.as_posix(),
                "chunk_count": plan["chunk_count"],
                "run_count": plan["run_count"],
            }
        if write_monthly:
            script_path, plan = stage_hrrr_plan(
                spec,
                label=spec.hrrr_label,
                windows=monthly_windows,
                chunk_hours=chunk_hours,
                threads=threads,
                hrrr_out_root=hrrr_out_root,
            )
            monthly_scripts.append(script_path)
            domain_summary["monthly_hrrr"] = {
                "plan_path": (hrrr_out_root / spec.hrrr_label / "plan.json").as_posix(),
                "script_path": script_path.as_posix(),
                "chunk_count": plan["chunk_count"],
                "run_count": plan["run_count"],
            }
        if write_controlled:
            script_path, summary = stage_controlled_plan(
                spec,
                controlled_root=controlled_root,
                profile=controlled_profile,
                threads=threads,
            )
            controlled_scripts.append(script_path)
            domain_summary["controlled"] = summary
        domain_summaries.append(domain_summary)

    if smoke_scripts:
        write_runner(stage_dir / "run_smoke_all.sh", smoke_scripts, repo_root=repo_root, title="Running ML terrain smoke HRRR pairs")
    if monthly_scripts:
        write_runner(stage_dir / "run_monthly_hrrr_all.sh", monthly_scripts, repo_root=repo_root, title="Running ML terrain monthly HRRR pairs")
        write_parallel_runner(
            stage_dir / "run_monthly_hrrr_parallel.sh",
            monthly_scripts,
            repo_root=repo_root,
            title="Running ML terrain monthly HRRR pairs in parallel",
        )
    if controlled_scripts:
        write_runner(stage_dir / "run_controlled_all.sh", controlled_scripts, repo_root=repo_root, title="Running ML terrain controlled matrices")
        write_parallel_runner(
            stage_dir / "run_controlled_parallel.sh",
            controlled_scripts,
            repo_root=repo_root,
            title="Running ML terrain controlled matrices in parallel",
        )

    summary = {
        "created_at_utc": dt.datetime.now(UTC).isoformat(),
        "stage_dir": stage_dir.as_posix(),
        "domains": [spec.domain for spec in specs],
        "threads": threads,
        "chunk_hours": chunk_hours,
        "monthly_days": monthly_days,
        "controlled_profile": controlled_profile,
        "fetch_script": fetch_script.as_posix() if fetch_script else "",
        "smoke_runner": (stage_dir / "run_smoke_all.sh").as_posix() if smoke_scripts else "",
        "monthly_hrrr_runner": (stage_dir / "run_monthly_hrrr_all.sh").as_posix() if monthly_scripts else "",
        "monthly_hrrr_parallel_runner": (
            stage_dir / "run_monthly_hrrr_parallel.sh"
        ).as_posix() if monthly_scripts else "",
        "controlled_runner": (stage_dir / "run_controlled_all.sh").as_posix() if controlled_scripts else "",
        "controlled_parallel_runner": (
            stage_dir / "run_controlled_parallel.sh"
        ).as_posix() if controlled_scripts else "",
        "domain_summaries": domain_summaries,
    }
    (stage_dir / "terrain_expansion_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage Copper/Vail/Monarch terrain data-generation scripts for ML training."
    )
    parser.add_argument(
        "--domain",
        action="append",
        choices=sorted(specs_by_domain()),
        help="Domain to stage. Repeat to select a subset. Defaults to all new terrain boxes.",
    )
    parser.add_argument("--label", default=DEFAULT_STAGE_LABEL)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT.as_posix())
    parser.add_argument("--hrrr-out-root", default=DEFAULT_HRRR_OUT_ROOT.as_posix())
    parser.add_argument("--controlled-root", default=DEFAULT_CONTROLLED_ROOT.as_posix())
    parser.add_argument("--smoke-start", default="202601010000")
    parser.add_argument("--smoke-end", default="202601020000")
    parser.add_argument("--monthly-days", type=int, default=7)
    parser.add_argument("--chunk-hours", type=int, default=24)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--controlled-profile",
        choices=["pilot", "standard", "dense", "training", "extreme"],
        default="training",
    )
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--no-smoke", action="store_true")
    parser.add_argument("--no-monthly", action="store_true")
    parser.add_argument("--no-controlled", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = stage_terrain_expansion(
        domains=args.domain,
        out_root=Path(args.out_root),
        label=args.label,
        hrrr_out_root=Path(args.hrrr_out_root),
        controlled_root=Path(args.controlled_root),
        smoke_start=args.smoke_start,
        smoke_end=args.smoke_end,
        monthly_days=args.monthly_days,
        chunk_hours=args.chunk_hours,
        threads=args.threads,
        controlled_profile=args.controlled_profile,
        write_fetch=not args.no_fetch,
        write_smoke=not args.no_smoke,
        write_monthly=not args.no_monthly,
        write_controlled=not args.no_controlled,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

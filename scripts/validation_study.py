#!/usr/bin/env python3
"""Run chunked WindNinja/HRRR/Synoptic validation studies."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from . import config_loader, raster_validation, synoptic_validation as sv, utils
    from .archive_manager import build_output_dir_name
except ImportError:
    import config_loader
    import raster_validation
    import synoptic_validation as sv
    import utils
    from archive_manager import build_output_dir_name


logger = utils.setup_logging("validation_study")
UTC = dt.timezone.utc
STUDY_CONFIG_DIR = config_loader.BASE_DIR / "config" / "studies"


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


@dataclass(frozen=True)
class StudyConfig:
    key: str
    label: str
    domain: str
    model: str
    chunk_hours: int
    tolerance_minutes: int
    speed_units: str
    default_height_m: float | None
    padding_km: float
    validation_root: Path
    station_manifest: Path
    metadata_file: Path
    bbox_file: Path


def resolve_repo_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (config_loader.BASE_DIR / path).resolve()


def ymdhm(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def parse_utc(raw_value: str) -> dt.datetime:
    parsed = sv.parse_utc_timestamp(raw_value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_study_config(study_key: str) -> StudyConfig:
    config_path = STUDY_CONFIG_DIR / f"{study_key}.json"
    if not config_path.exists():
        raise ValueError(f"Study config not found: {config_path}")
    payload = json.loads(config_path.read_text(encoding="utf-8"))

    validation_root = resolve_repo_path(
        payload.get("validation_root", f"runtime/validation/{study_key}")
    )
    station_manifest = resolve_repo_path(
        payload.get("station_manifest", validation_root / "stations.csv")
    )
    metadata_file = resolve_repo_path(
        payload.get("metadata_file", validation_root / "station_metadata.json")
    )
    bbox_file = resolve_repo_path(
        payload.get("bbox_file", validation_root / "station_bbox.json")
    )

    return StudyConfig(
        key=payload.get("key", study_key),
        label=payload.get("label", study_key),
        domain=payload["domain"],
        model=payload.get("model", "HRRR"),
        chunk_hours=int(payload.get("chunk_hours", 24)),
        tolerance_minutes=int(payload.get("tolerance_minutes", 30)),
        speed_units=payload.get("speed_units", "mph"),
        default_height_m=payload.get("default_height_m"),
        padding_km=float(payload.get("padding_km", 2.0)),
        validation_root=validation_root,
        station_manifest=station_manifest,
        metadata_file=metadata_file,
        bbox_file=bbox_file,
    )


def plan_chunks(start: dt.datetime, end: dt.datetime, chunk_hours: int) -> list[Chunk]:
    if start >= end:
        raise ValueError("--end must be later than --start.")
    if start.minute or end.minute or start.second or end.second or start.microsecond or end.microsecond:
        raise ValueError("--start and --end must be exact UTC hour boundaries.")
    if chunk_hours < 1:
        raise ValueError("--chunk-hours must be >= 1.")

    chunks = []
    cursor = start
    delta = dt.timedelta(hours=chunk_hours)
    while cursor < end:
        chunk_end = min(cursor + delta, end)
        chunks.append(Chunk(cursor, chunk_end))
        cursor = chunk_end
    return chunks


def run_dir_for_chunk(study: StudyConfig, chunk: Chunk) -> Path:
    run_label = f"reanalysis_{chunk.hours}h"
    return Path(config_loader.TEMP_DIR) / build_output_dir_name(
        study.domain,
        chunk.start.replace(tzinfo=None),
        run_label,
        study.model,
    )


def chunk_output_paths(study: StudyConfig, chunk: Chunk) -> dict[str, Path]:
    chunk_dir = study.validation_root / "chunks" / chunk.label
    return {
        "dir": chunk_dir,
        "samples": chunk_dir / "samples.csv",
        "station_summary": chunk_dir / "station_summary.csv",
        "group_summary": chunk_dir / "group_summary.csv",
        "summary": chunk_dir / "summary.json",
    }


def ensure_station_inputs(
    study: StudyConfig,
    start: dt.datetime,
    end: dt.datetime,
    token: str | None,
) -> None:
    if not study.station_manifest.exists():
        raise ValueError(f"Station manifest does not exist: {study.station_manifest}")
    logger.info(f"Using station manifest: {study.station_manifest}")

    prep_args = argparse.Namespace(
        station_file=str(study.station_manifest),
        points_output=str(study.validation_root / "points.csv"),
        metadata_output=str(study.metadata_file),
        bbox_output=str(study.bbox_file),
        padding_km=study.padding_km,
        default_height=study.default_height_m,
        start=ymdhm(start),
        end=ymdhm(end),
        token=token,
    )
    sv.prepare_points(prep_args)


def run_command(command: list[str], *, dry_run: bool = False) -> None:
    logger.info(" ".join(command))
    if dry_run:
        return
    subprocess.run(command, check=True)


def run_preflight(study: StudyConfig, *, dry_run: bool = False) -> None:
    run_command(
        [
            sys.executable,
            str(config_loader.SCRIPTS_DIR / "preflight_check.py"),
            "--domain",
            study.domain,
        ],
        dry_run=dry_run,
    )


def run_reanalysis_chunk(
    study: StudyConfig,
    chunk: Chunk,
    *,
    force: bool,
    dry_run: bool,
) -> Path:
    run_dir = run_dir_for_chunk(study, chunk)
    if run_dir.exists() and not force:
        logger.info(f"Using existing run directory: {run_dir}")
        return run_dir

    run_command(
        [
            sys.executable,
            str(config_loader.SCRIPTS_DIR / "daily_run.py"),
            "--mode",
            "reanalysis",
            "--start",
            ymdhm(chunk.start),
            "--end",
            ymdhm(chunk.end),
            "--model",
            study.model,
            "--domain",
            study.domain,
            "--keep-temp",
            "--no-upload",
        ],
        dry_run=dry_run,
    )
    return run_dir


def validate_chunk(
    study: StudyConfig,
    chunk: Chunk,
    run_dir: Path,
    *,
    force: bool,
    dry_run: bool,
) -> Path:
    paths = chunk_output_paths(study, chunk)
    utils.ensure_dir(str(paths["dir"]))

    if paths["summary"].exists() and paths["samples"].exists() and not force:
        logger.info(f"Using existing chunk validation: {paths['summary']}")
        return paths["samples"]

    run_command(
        [
            sys.executable,
            str(config_loader.SCRIPTS_DIR / "raster_validation.py"),
            "--run-dir",
            str(run_dir),
            "--metadata-file",
            str(study.metadata_file),
            "--start",
            ymdhm(chunk.start),
            "--end",
            ymdhm(chunk.end),
            "--samples-csv",
            str(paths["samples"]),
            "--station-summary-csv",
            str(paths["station_summary"]),
            "--group-summary-csv",
            str(paths["group_summary"]),
            "--summary-json",
            str(paths["summary"]),
            "--tolerance-minutes",
            str(study.tolerance_minutes),
            "--speed-units",
            study.speed_units,
        ],
        dry_run=dry_run,
    )
    return paths["samples"]


def load_sample_rows(paths: list[Path]) -> list[dict]:
    rows = []
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                for key in (
                    "height_m",
                    "speed_obs",
                    "dir_obs_deg",
                    "u_obs",
                    "v_obs",
                    "wn_speed",
                    "wn_dir_deg",
                    "wn_u",
                    "wn_v",
                    "wn_speed_error",
                    "wn_dir_abs_error_deg",
                    "wn_vector_error",
                    "wx_speed",
                    "wx_dir_deg",
                    "wx_u",
                    "wx_v",
                    "wx_speed_error",
                    "wx_dir_abs_error_deg",
                    "wx_vector_error",
                ):
                    if key in row and row[key] != "":
                        row[key] = float(row[key])
                rows.append(row)
    return rows


def aggregate_outputs(study: StudyConfig, chunks: list[Chunk], sample_paths: list[Path]) -> None:
    sample_rows = load_sample_rows(sample_paths)
    if not sample_rows:
        raise ValueError("No sample rows found to aggregate.")

    output_paths = {
        "samples": study.validation_root / "samples.csv",
        "station_summary": study.validation_root / "station_summary.csv",
        "group_summary": study.validation_root / "group_summary.csv",
        "summary": study.validation_root / "summary.json",
    }

    station_rows = raster_validation.summary_rows(sample_rows, "station_id")
    group_rows = raster_validation.summary_rows(sample_rows, "group")
    overall = sv.summarize_samples(sample_rows)

    sv.rows_to_csv(output_paths["samples"], sample_rows)
    sv.rows_to_csv(output_paths["station_summary"], station_rows)
    sv.rows_to_csv(output_paths["group_summary"], group_rows)
    sv.write_json(
        output_paths["summary"],
        {
            "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
            "study": study.key,
            "label": study.label,
            "domain": study.domain,
            "model": study.model,
            "chunk_count": len(chunks),
            "chunks": [
                {
                    "start_utc": sv.isoformat_utc(chunk.start),
                    "end_utc": sv.isoformat_utc(chunk.end),
                    "samples_csv": str(path),
                }
                for chunk, path in zip(chunks, sample_paths)
            ],
            "tolerance_minutes": study.tolerance_minutes,
            "speed_units": study.speed_units,
            "matched_sample_count": len(sample_rows),
            "matched_station_count": len({row["station_id"] for row in sample_rows}),
            "overall": overall,
        },
    )

    logger.info(f"Wrote aggregate samples CSV: {output_paths['samples']}")
    logger.info(f"Wrote aggregate station summary CSV: {output_paths['station_summary']}")
    logger.info(f"Wrote aggregate group summary CSV: {output_paths['group_summary']}")
    logger.info(f"Wrote aggregate summary JSON: {output_paths['summary']}")


def print_plan(study: StudyConfig, chunks: list[Chunk]) -> None:
    payload = {
        "study": study.key,
        "label": study.label,
        "domain": study.domain,
        "model": study.model,
        "station_manifest": str(study.station_manifest),
        "metadata_file": str(study.metadata_file),
        "validation_root": str(study.validation_root),
        "chunk_hours": study.chunk_hours,
        "chunk_count": len(chunks),
        "chunks": [
            {
                "start": ymdhm(chunk.start),
                "end": ymdhm(chunk.end),
                "hours": chunk.hours,
                "run_dir": str(run_dir_for_chunk(study, chunk)),
                "samples_csv": str(chunk_output_paths(study, chunk)["samples"]),
            }
            for chunk in chunks
        ],
    }
    print(json.dumps(payload, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a chunked WindNinja validation study."
    )
    parser.add_argument("study", help="Study key from config/studies/<key>.json.")
    parser.add_argument("--start", required=True, help="UTC start time.")
    parser.add_argument("--end", help="UTC end time.")
    parser.add_argument(
        "--pilot-hours",
        type=int,
        help="Use a short pilot window starting at --start instead of passing --end.",
    )
    parser.add_argument("--chunk-hours", type=int, help="Override study chunk size.")
    parser.add_argument("--model", help="Override study weather model.")
    parser.add_argument("--domain", help="Override study domain.")
    parser.add_argument("--tolerance-minutes", type=int, help="Override observation match tolerance.")
    parser.add_argument("--speed-units", choices=["mph", "mps", "kph", "kts"])
    parser.add_argument("--default-height", type=float, help="Fallback station wind height in meters.")
    parser.add_argument("--token", help="Synoptic API token. Defaults to MWN_SYNOPTIC_TOKEN.")
    parser.add_argument("--plan", action="store_true", help="Print the chunk plan and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument("--force", action="store_true", help="Rerun completed chunks and validations.")
    parser.add_argument("--skip-runs", action="store_true", help="Validate existing run directories only.")
    parser.add_argument("--no-preflight", action="store_true", help="Skip preflight validation.")
    return parser


def with_overrides(study: StudyConfig, args: argparse.Namespace) -> StudyConfig:
    return StudyConfig(
        key=study.key,
        label=study.label,
        domain=args.domain or study.domain,
        model=args.model or study.model,
        chunk_hours=args.chunk_hours or study.chunk_hours,
        tolerance_minutes=args.tolerance_minutes or study.tolerance_minutes,
        speed_units=args.speed_units or study.speed_units,
        default_height_m=(
            args.default_height
            if args.default_height is not None
            else study.default_height_m
        ),
        padding_km=study.padding_km,
        validation_root=study.validation_root,
        station_manifest=study.station_manifest,
        metadata_file=study.metadata_file,
        bbox_file=study.bbox_file,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    study = with_overrides(load_study_config(args.study), args)
    if study.model.upper() != "HRRR":
        parser.error(
            "validate-study supports historical validation only for HRRR. "
            "Use run --model NBM for native NBM forecast runs."
        )

    start = parse_utc(args.start)
    if args.pilot_hours is not None:
        if args.end:
            parser.error("--pilot-hours cannot be combined with --end.")
        if args.pilot_hours < 1:
            parser.error("--pilot-hours must be >= 1.")
        end = start + dt.timedelta(hours=args.pilot_hours)
    elif args.end:
        end = parse_utc(args.end)
    else:
        parser.error("Pass --end or --pilot-hours.")

    try:
        chunks = plan_chunks(start, end, study.chunk_hours)
    except ValueError as exc:
        parser.error(str(exc))

    if args.plan or args.dry_run:
        print_plan(study, chunks)
        if args.plan:
            return 0

    if not args.no_preflight:
        try:
            run_preflight(study, dry_run=args.dry_run)
        except subprocess.CalledProcessError as exc:
            logger.error(f"Preflight failed for domain {study.domain}.")
            return exc.returncode

    if args.dry_run:
        for chunk in chunks:
            run_reanalysis_chunk(study, chunk, force=args.force, dry_run=True)
            validate_chunk(
                study,
                chunk,
                run_dir_for_chunk(study, chunk),
                force=args.force,
                dry_run=True,
            )
        return 0

    ensure_station_inputs(
        study,
        start,
        end,
        args.token,
    )

    sample_paths = []
    for chunk in chunks:
        run_dir = run_dir_for_chunk(study, chunk)
        try:
            if not args.skip_runs:
                run_dir = run_reanalysis_chunk(
                    study,
                    chunk,
                    force=args.force,
                    dry_run=False,
                )
            sample_paths.append(
                validate_chunk(
                    study,
                    chunk,
                    run_dir,
                    force=args.force,
                    dry_run=False,
                )
            )
        except subprocess.CalledProcessError as exc:
            logger.error(f"Validation study command failed: {' '.join(exc.cmd)}")
            return exc.returncode

    aggregate_outputs(study, chunks, sample_paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

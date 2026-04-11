#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REQUIRED_BAND_COUNT = 8
LCP_BANDS = tuple(range(1, REQUIRED_BAND_COUNT + 1))
LCP_CREATION_OPTIONS = (
    "ELEVATION_UNIT=METERS",
    "SLOPE_UNIT=DEGREES",
    "ASPECT_UNIT=AZIMUTH_DEGREES",
    "CANOPY_COV_UNIT=PERCENT",
    "CANOPY_HT_UNIT=METERS_X_10",
    "CBH_UNIT=METERS_X_10",
    "CBD_UNIT=KG_PER_CUBIC_METER_X_100",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert an IFTDSS/LANDFIRE-style landscape GeoTIFF into a WindNinja-"
            "compatible LCP plus .prj sidecar using GDAL."
        )
    )
    parser.add_argument("input_tif", type=Path, help="Path to the source landscape GeoTIFF.")
    parser.add_argument("output_lcp", type=Path, help="Path to the output .lcp file.")
    parser.add_argument(
        "--description",
        default="",
        help="Optional dataset description to embed in the LCP header.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output LCP and PRJ if they already exist.",
    )
    return parser.parse_args()


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True)


def output_prj_path(output_lcp: Path) -> Path:
    return output_lcp.with_suffix(".prj")


def ensure_input_exists(input_tif: Path) -> None:
    if not input_tif.is_file():
        raise FileNotFoundError(f"Input GeoTIFF not found: {input_tif}")


def ensure_output_path(output_lcp: Path, force: bool) -> None:
    if output_lcp.suffix.lower() != ".lcp":
        raise ValueError(f"Output file must end with .lcp: {output_lcp}")
    output_prj = output_prj_path(output_lcp)
    if not force:
        for candidate in (output_lcp, output_prj):
            if candidate.exists():
                raise FileExistsError(
                    f"Refusing to overwrite existing file without --force: {candidate}"
                )
    output_lcp.parent.mkdir(parents=True, exist_ok=True)


def ensure_lcp_driver_available() -> None:
    result = run_command(["gdalinfo", "--formats"])
    if " LCP " not in result.stdout and " LCP" not in result.stdout:
        raise RuntimeError("GDAL LCP driver is not available in this environment.")


def get_band_count(input_tif: Path) -> int:
    result = run_command(["gdalinfo", "-json", str(input_tif)])
    metadata = json.loads(result.stdout)
    return len(metadata.get("bands", []))


def validate_band_count(band_count: int) -> None:
    if band_count < REQUIRED_BAND_COUNT:
        raise ValueError(
            f"Landscape GeoTIFF must have at least {REQUIRED_BAND_COUNT} bands; "
            f"found {band_count}."
        )


def build_translate_command(input_tif: Path, output_lcp: Path, description: str) -> list[str]:
    command = ["gdal_translate", "-of", "LCP"]
    for band in LCP_BANDS:
        command.extend(["-b", str(band)])
    for option in LCP_CREATION_OPTIONS:
        command.extend(["-co", option])
    if description:
        command.extend(["-co", f"DESCRIPTION={description}"])
    command.extend([str(input_tif), str(output_lcp)])
    return command


def write_prj_sidecar(input_tif: Path, output_lcp: Path) -> Path:
    output_prj = output_prj_path(output_lcp)
    result = run_command(["gdalsrsinfo", "-o", "wkt_esri", str(input_tif)])
    output_prj.write_text(result.stdout.strip() + "\n", encoding="utf-8")
    return output_prj


def convert_to_lcp(input_tif: Path, output_lcp: Path, description: str) -> Path:
    run_command(build_translate_command(input_tif, output_lcp, description))
    return write_prj_sidecar(input_tif, output_lcp)


def main() -> int:
    args = parse_args()
    description = args.description or args.input_tif.stem

    try:
        ensure_input_exists(args.input_tif)
        ensure_output_path(args.output_lcp, args.force)
        ensure_lcp_driver_available()
        band_count = get_band_count(args.input_tif)
        validate_band_count(band_count)
        output_prj = convert_to_lcp(args.input_tif, args.output_lcp, description)
    except (
        subprocess.CalledProcessError,
        FileExistsError,
        FileNotFoundError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Created {args.output_lcp}")
    print(f"Created {output_prj}")
    if band_count > REQUIRED_BAND_COUNT:
        print(
            f"Used bands 1-{REQUIRED_BAND_COUNT} from a {band_count}-band source GeoTIFF.",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

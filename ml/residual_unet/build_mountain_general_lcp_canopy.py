"""Build mountain-general datasets with one LCP canopy channel."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .build_combined_dataset import build_combined_dataset
from .build_controlled_dataset import build_controlled_dataset
from .build_dataset import LCP_CANOPY_CHANNEL, build_dataset


DEFAULT_BASE_DATASET_NAME = "mountain_general_9p6_lcp_canopy_v1"
DEFAULT_V2_DATASET_NAME = "mountain_general_9p6_lcp_canopy_v2"
DEFAULT_PROCESSED_ROOT = Path("ml/residual_unet/data/processed")
DEFAULT_CONTROLLED_ROOT = Path("runtime/ml/residual_unet/raw/controlled_9p6_15deg")


@dataclass(frozen=True)
class DomainSpec:
    domain: str
    mass_domain: str
    hrrr_source: str
    controlled_source: str


BASE_DOMAIN_SPECS = [
    DomainSpec(
        domain="berthoud_pass",
        mass_domain="berthoud_pass_mass",
        hrrr_source="berthoud_pass_hrrr_lcp_canopy_v1",
        controlled_source="berthoud_pass_controlled_lcp_canopy_9p6_15deg",
    ),
    DomainSpec(
        domain="breck_tenmile_9p6",
        mass_domain="breck_tenmile_9p6_mass",
        hrrr_source="breck_tenmile_9p6_hrrr_lcp_canopy_v1",
        controlled_source="breck_tenmile_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
    DomainSpec(
        domain="keystone_9p6",
        mass_domain="keystone_9p6_mass",
        hrrr_source="keystone_9p6_hrrr_lcp_canopy_v1",
        controlled_source="keystone_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
    DomainSpec(
        domain="loveland_abasin_9p6",
        mass_domain="loveland_abasin_9p6_mass",
        hrrr_source="loveland_abasin_9p6_hrrr_lcp_canopy_v1",
        controlled_source="loveland_abasin_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
]

EXPANSION_DOMAIN_SPECS = [
    DomainSpec(
        domain="copper_mountain_9p6",
        mass_domain="copper_mountain_9p6_mass",
        hrrr_source="copper_mountain_9p6_hrrr_lcp_canopy_v1",
        controlled_source="copper_mountain_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
    DomainSpec(
        domain="vail_central_9p6",
        mass_domain="vail_central_9p6_mass",
        hrrr_source="vail_central_9p6_hrrr_lcp_canopy_v1",
        controlled_source="vail_central_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
    DomainSpec(
        domain="monarch_pass_9p6",
        mass_domain="monarch_pass_9p6_mass",
        hrrr_source="monarch_pass_9p6_hrrr_lcp_canopy_v1",
        controlled_source="monarch_pass_9p6_controlled_lcp_canopy_9p6_15deg",
    ),
]

DOMAIN_SETS = {
    "base4": BASE_DOMAIN_SPECS,
    "expansion3": EXPANSION_DOMAIN_SPECS,
    "base4_plus_expansion3": BASE_DOMAIN_SPECS + EXPANSION_DOMAIN_SPECS,
}


def build_mountain_general_lcp_canopy(
    *,
    source_root: Path,
    processed_root: Path,
    controlled_root: Path,
    out_dir: Path,
    crop_size: int,
    force: bool,
    domain_specs: Sequence[DomainSpec] = BASE_DOMAIN_SPECS,
) -> dict:
    processed_root.mkdir(parents=True, exist_ok=True)
    terrain_features = [LCP_CANOPY_CHANNEL]
    sources: list[tuple[str, Path]] = []
    source_summaries: dict[str, dict] = {}

    for spec in domain_specs:
        source_dir = processed_root / spec.hrrr_source
        summary = build_dataset(
            source_root,
            source_dir,
            crop_size,
            force=force,
            momentum_domain=spec.domain,
            mass_domain=spec.mass_domain,
            terrain_features=terrain_features,
            source_dataset=spec.hrrr_source,
            sample_prefix=spec.hrrr_source,
        )
        sources.append((spec.hrrr_source, source_dir))
        source_summaries[spec.hrrr_source] = summary

    for spec in domain_specs:
        raw_root = controlled_root / spec.domain
        source_dir = processed_root / spec.controlled_source
        summary = build_controlled_dataset(
            raw_root,
            source_dir,
            crop_size,
            force=force,
            terrain_domain=spec.domain,
            terrain_features=terrain_features,
            source_dataset=spec.controlled_source,
        )
        sources.append((spec.controlled_source, source_dir))
        source_summaries[spec.controlled_source] = summary

    combined = build_combined_dataset(
        sources[0][1],
        sources[1][1],
        out_dir,
        force=force,
        sources=sources,
    )
    return {
        "dataset": out_dir.name,
        "domains": [spec.domain for spec in domain_specs],
        "terrain_features": terrain_features,
        "source_summaries": source_summaries,
        "combined_summary": combined,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build 9.6 km mountain-general LCP canopy datasets."
    )
    parser.add_argument("--source-root", default=".", help="Mountain WindNinja repo root.")
    parser.add_argument("--processed-root", default=DEFAULT_PROCESSED_ROOT.as_posix())
    parser.add_argument("--controlled-root", default=DEFAULT_CONTROLLED_ROOT.as_posix())
    parser.add_argument(
        "--domain-set",
        choices=sorted(DOMAIN_SETS),
        default="base4",
        help=(
            "Which domain group to combine. base4 preserves the current dataset; "
            "base4_plus_expansion3 creates the planned seven-domain V2 set."
        ),
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output combined processed dataset directory.",
    )
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--force", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    domain_specs = DOMAIN_SETS[args.domain_set]
    default_name = (
        DEFAULT_BASE_DATASET_NAME
        if args.domain_set == "base4"
        else DEFAULT_V2_DATASET_NAME
    )
    out_dir = Path(args.out) if args.out else DEFAULT_PROCESSED_ROOT / default_name
    summary = build_mountain_general_lcp_canopy(
        source_root=Path(args.source_root).resolve(),
        processed_root=Path(args.processed_root),
        controlled_root=Path(args.controlled_root),
        out_dir=out_dir,
        crop_size=args.crop_size,
        force=args.force,
        domain_specs=domain_specs,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

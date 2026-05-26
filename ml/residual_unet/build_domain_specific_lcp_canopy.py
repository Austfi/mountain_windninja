"""Build terrain-specific LCP-canopy datasets for site-tuned residual U-Nets."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .build_combined_dataset import build_combined_dataset
from .build_controlled_dataset import build_controlled_dataset
from .build_dataset import LCP_CANOPY_CHANNEL, build_dataset


DEFAULT_PROCESSED_ROOT = Path("ml/residual_unet/data/processed")
DEFAULT_CONTROLLED_15_ROOT = Path("runtime/ml/residual_unet/raw/controlled_9p6_15deg")
DEFAULT_CONTROLLED_MIDPOINT_ROOT = Path("runtime/ml/residual_unet/raw/controlled_9p6_7p5_midpoints")


@dataclass(frozen=True)
class DomainSpec:
    domain: str
    mass_domain: str
    dataset_name: str
    hrrr_source: str
    controlled_15_source: str
    controlled_midpoint_source: str


DOMAIN_SPECS = {
    "breck": DomainSpec(
        domain="breck_tenmile_9p6",
        mass_domain="breck_tenmile_9p6_mass",
        dataset_name="breck_tenmile_9p6_specific_lcp_canopy_v1",
        hrrr_source="breck_tenmile_9p6_hrrr_specific_lcp_canopy_v1",
        controlled_15_source="breck_tenmile_9p6_controlled_lcp_canopy_9p6_15deg",
        controlled_midpoint_source="breck_tenmile_9p6_controlled_lcp_canopy_9p6_7p5deg_midpoints_v1",
    ),
    "keystone": DomainSpec(
        domain="keystone_9p6",
        mass_domain="keystone_9p6_mass",
        dataset_name="keystone_9p6_specific_lcp_canopy_v1",
        hrrr_source="keystone_9p6_hrrr_specific_lcp_canopy_v1",
        controlled_15_source="keystone_9p6_controlled_lcp_canopy_9p6_15deg",
        controlled_midpoint_source="keystone_9p6_controlled_lcp_canopy_9p6_7p5deg_midpoints_v1",
    ),
}


def build_domain_specific_lcp_canopy(
    *,
    spec: DomainSpec,
    source_root: Path,
    processed_root: Path,
    controlled_15_root: Path,
    controlled_midpoint_root: Path,
    out_dir: Path,
    crop_size: int,
    force: bool,
    include_midpoint_controlled: bool,
) -> dict:
    processed_root.mkdir(parents=True, exist_ok=True)
    terrain_features = [LCP_CANOPY_CHANNEL]
    sources: list[tuple[str, Path]] = []
    source_summaries: dict[str, dict] = {}

    hrrr_dir = processed_root / spec.hrrr_source
    hrrr_summary = build_dataset(
        source_root,
        hrrr_dir,
        crop_size,
        force=force,
        momentum_domain=spec.domain,
        mass_domain=spec.mass_domain,
        terrain_features=terrain_features,
        source_dataset=spec.hrrr_source,
        sample_prefix=spec.hrrr_source,
    )
    sources.append((spec.hrrr_source, hrrr_dir))
    source_summaries[spec.hrrr_source] = hrrr_summary

    controlled_15_dir = processed_root / spec.controlled_15_source
    controlled_15_summary = build_controlled_dataset(
        controlled_15_root / spec.domain,
        controlled_15_dir,
        crop_size,
        force=force,
        terrain_domain=spec.domain,
        terrain_features=terrain_features,
        source_dataset=spec.controlled_15_source,
    )
    sources.append((spec.controlled_15_source, controlled_15_dir))
    source_summaries[spec.controlled_15_source] = controlled_15_summary

    if include_midpoint_controlled:
        controlled_midpoint_dir = processed_root / spec.controlled_midpoint_source
        controlled_midpoint_summary = build_controlled_dataset(
            controlled_midpoint_root / spec.domain,
            controlled_midpoint_dir,
            crop_size,
            force=force,
            terrain_domain=spec.domain,
            terrain_features=terrain_features,
            source_dataset=spec.controlled_midpoint_source,
        )
        sources.append((spec.controlled_midpoint_source, controlled_midpoint_dir))
        source_summaries[spec.controlled_midpoint_source] = controlled_midpoint_summary

    combined = build_combined_dataset(
        sources[0][1],
        sources[1][1],
        out_dir,
        force=force,
        sources=sources,
    )
    return {
        "dataset": out_dir.name,
        "domain": spec.domain,
        "mass_domain": spec.mass_domain,
        "terrain_features": terrain_features,
        "source_summaries": source_summaries,
        "combined_summary": combined,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a Breck or Keystone-specific LCP-canopy residual U-Net dataset."
    )
    parser.add_argument("--domain", choices=sorted(DOMAIN_SPECS), required=True)
    parser.add_argument("--source-root", default=".", help="Mountain WindNinja repo root.")
    parser.add_argument("--processed-root", default=DEFAULT_PROCESSED_ROOT.as_posix())
    parser.add_argument("--controlled-15-root", default=DEFAULT_CONTROLLED_15_ROOT.as_posix())
    parser.add_argument("--controlled-midpoint-root", default=DEFAULT_CONTROLLED_MIDPOINT_ROOT.as_posix())
    parser.add_argument("--out", help="Output combined processed dataset directory.")
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--skip-midpoint-controlled",
        action="store_true",
        help="Build with only the existing 15-degree controlled matrix.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    spec = DOMAIN_SPECS[args.domain]
    out_dir = Path(args.out) if args.out else Path(args.processed_root) / spec.dataset_name
    summary = build_domain_specific_lcp_canopy(
        spec=spec,
        source_root=Path(args.source_root).resolve(),
        processed_root=Path(args.processed_root),
        controlled_15_root=Path(args.controlled_15_root),
        controlled_midpoint_root=Path(args.controlled_midpoint_root),
        out_dir=out_dir,
        crop_size=args.crop_size,
        force=args.force,
        include_midpoint_controlled=not args.skip_midpoint_controlled,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

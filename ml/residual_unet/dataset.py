"""PyTorch dataset utilities for residual U-Net NPZ shards."""
from __future__ import annotations

import csv
import json
from pathlib import Path


def load_manifest(processed_dir: str | Path) -> list[dict[str, str]]:
    manifest_path = Path(processed_dir) / "manifest.csv"
    with manifest_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_normalization(processed_dir: str | Path) -> dict:
    path = Path(processed_dir) / "normalization.json"
    return json.loads(path.read_text(encoding="utf-8"))


def filter_rows(
    rows: list[dict[str, str]],
    split: str,
    *,
    source_dataset: str | None = None,
    source_datasets: list[str] | None = None,
    exclude_source_datasets: list[str] | None = None,
    max_samples: int | None = None,
) -> list[dict[str, str]]:
    selected = [row for row in rows if row["split"] == split]
    include_sources = set(source_datasets or [])
    if source_dataset is not None:
        include_sources.add(source_dataset)
    if include_sources:
        selected = [row for row in selected if row.get("source_dataset") in include_sources]
    if exclude_source_datasets:
        exclude_sources = set(exclude_source_datasets)
        selected = [row for row in selected if row.get("source_dataset") not in exclude_sources]
    if max_samples is not None:
        selected = selected[:max_samples]
    return selected


class ResidualWindDataset:
    """Lazy NPZ-backed torch dataset.

    Imports torch lazily so the rest of this repo can run without ML dependencies.
    """

    def __init__(
        self,
        processed_dir: str | Path,
        rows: list[dict[str, str]],
        normalization: dict,
    ) -> None:
        import numpy as np

        self.processed_dir = Path(processed_dir)
        self.rows = rows
        self.input_mean = np.asarray(normalization["input_mean"], dtype=np.float32)[:, None, None]
        self.input_std = np.asarray(normalization["input_std"], dtype=np.float32)[:, None, None]

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict:
        import numpy as np
        import torch

        row = self.rows[index]
        with np.load(self.processed_dir / row["npz_path"]) as sample:
            x = sample["x"].astype(np.float32)
            y = sample["y"].astype(np.float32)
            mass_uv = sample["mass_uv"].astype(np.float32)
            mom_uv = sample["mom_uv"].astype(np.float32)
            valid_mask = sample["valid_mask"].astype(np.float32)

        x = (x - self.input_mean) / np.maximum(self.input_std, 1e-6)
        return {
            "x": torch.from_numpy(x),
            "y": torch.from_numpy(y),
            "mass_uv": torch.from_numpy(mass_uv),
            "mom_uv": torch.from_numpy(mom_uv),
            "valid_mask": torch.from_numpy(valid_mask),
            "sample_id": row["sample_id"],
            "source_dataset": row.get("source_dataset", ""),
            "source_sample_id": row.get("source_sample_id", ""),
            "timestamp_utc": row.get("timestamp_utc", row["sample_id"]),
            "date": row.get("date", ""),
            "case_id": row.get("case_id", ""),
            "speed_mps": row.get("speed_mps", ""),
            "direction_deg": row.get("direction_deg", ""),
            "domain": row.get("domain", ""),
            "mass_domain": row.get("mass_domain", ""),
            "momentum_domain": row.get("momentum_domain", ""),
        }


def make_dataloader(
    processed_dir: str | Path,
    split: str,
    normalization: dict,
    *,
    batch_size: int,
    num_workers: int = 0,
    shuffle: bool = False,
    source_dataset: str | None = None,
    source_datasets: list[str] | None = None,
    exclude_source_datasets: list[str] | None = None,
    max_samples: int | None = None,
    pin_memory: bool | None = None,
    persistent_workers: bool | None = None,
    prefetch_factor: int | None = None,
):
    import torch

    rows = filter_rows(
        load_manifest(processed_dir),
        split,
        source_dataset=source_dataset,
        source_datasets=source_datasets,
        exclude_source_datasets=exclude_source_datasets,
        max_samples=max_samples,
    )
    dataset = ResidualWindDataset(processed_dir, rows, normalization)
    loader_kwargs = {
        "batch_size": batch_size,
        "shuffle": shuffle,
        "num_workers": num_workers,
        "pin_memory": torch.cuda.is_available() if pin_memory is None else pin_memory,
    }
    if num_workers > 0:
        loader_kwargs["persistent_workers"] = (
            True if persistent_workers is None else persistent_workers
        )
        if prefetch_factor is not None:
            loader_kwargs["prefetch_factor"] = prefetch_factor
    return torch.utils.data.DataLoader(dataset, **loader_kwargs)

"""Minimal AAIGrid raster I/O used by the WindNinja ML dataset builder."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AsciiGrid:
    data: object
    ncols: int
    nrows: int
    xllcorner: float
    yllcorner: float
    cellsize: float
    nodata: float

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        xmin = self.xllcorner
        ymin = self.yllcorner
        xmax = xmin + self.ncols * self.cellsize
        ymax = ymin + self.nrows * self.cellsize
        return xmin, ymin, xmax, ymax


def read_ascii_grid(path: str | Path) -> AsciiGrid:
    """Read an Arc/Info ASCII Grid into a float32 array plus georeference metadata."""
    import numpy as np

    grid_path = Path(path)
    header: dict[str, float] = {}
    header_lines = []
    with grid_path.open("r", encoding="utf-8") as f:
        while len(header_lines) < 6:
            line = f.readline()
            if not line:
                raise ValueError(f"Unexpected end of AAIGrid header: {grid_path}")
            parts = line.strip().split()
            if len(parts) < 2:
                raise ValueError(f"Invalid AAIGrid header line in {grid_path}: {line!r}")
            key = parts[0].lower()
            value = float(parts[1])
            header[key] = value
            header_lines.append(line)
        data = np.loadtxt(f, dtype=np.float32)

    ncols = int(header["ncols"])
    nrows = int(header["nrows"])
    if data.shape != (nrows, ncols):
        raise ValueError(f"AAIGrid data shape mismatch for {grid_path}: {data.shape} != {(nrows, ncols)}")

    cellsize = float(header["cellsize"])
    if "xllcorner" in header:
        xllcorner = float(header["xllcorner"])
    else:
        xllcorner = float(header["xllcenter"]) - cellsize / 2.0

    if "yllcorner" in header:
        yllcorner = float(header["yllcorner"])
    else:
        yllcorner = float(header["yllcenter"]) - cellsize / 2.0

    return AsciiGrid(
        data=data,
        ncols=ncols,
        nrows=nrows,
        xllcorner=xllcorner,
        yllcorner=yllcorner,
        cellsize=cellsize,
        nodata=float(header.get("nodata_value", -9999.0)),
    )


def write_ascii_grid(
    path: str | Path,
    grid: AsciiGrid,
    data=None,
    *,
    nodata: float | None = None,
    fmt: str = "%.6f",
) -> None:
    """Write an Arc/Info ASCII Grid using metadata from *grid*."""
    import numpy as np

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    values = np.asarray(grid.data if data is None else data)
    expected_shape = (grid.nrows, grid.ncols)
    if values.shape != expected_shape:
        raise ValueError(f"AAIGrid data shape mismatch for {output_path}: {values.shape} != {expected_shape}")

    nodata_value = grid.nodata if nodata is None else float(nodata)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(f"ncols         {grid.ncols}\n")
        f.write(f"nrows         {grid.nrows}\n")
        f.write(f"xllcorner     {grid.xllcorner:.12g}\n")
        f.write(f"yllcorner     {grid.yllcorner:.12g}\n")
        f.write(f"cellsize      {grid.cellsize:.12g}\n")
        f.write(f"NODATA_value  {nodata_value:.12g}\n")
        np.savetxt(f, values, fmt=fmt)


def same_grid(left: AsciiGrid, right: AsciiGrid, *, tolerance: float = 1e-6) -> bool:
    return (
        left.ncols == right.ncols
        and left.nrows == right.nrows
        and abs(left.xllcorner - right.xllcorner) <= tolerance
        and abs(left.yllcorner - right.yllcorner) <= tolerance
        and abs(left.cellsize - right.cellsize) <= tolerance
    )


def center_crop_offsets(height: int, width: int, crop_size: int) -> tuple[int, int, int]:
    if crop_size > height or crop_size > width:
        raise ValueError(f"Crop size {crop_size} exceeds array shape {(height, width)}")
    row0 = (height - crop_size) // 2
    col0 = (width - crop_size) // 2
    bottom_removed = height - crop_size - row0
    return row0, col0, bottom_removed


def center_crop(array, crop_size: int):
    import numpy as np

    values = np.asarray(array)
    height, width = values.shape[-2:]
    row0, col0, _bottom_removed = center_crop_offsets(height, width, crop_size)
    return values[..., row0:row0 + crop_size, col0:col0 + crop_size]


def crop_grid_metadata(grid: AsciiGrid, crop_size: int, data=None) -> AsciiGrid:
    """Return grid metadata for a center crop, preserving lower-left georeferencing."""
    row0, col0, bottom_removed = center_crop_offsets(grid.nrows, grid.ncols, crop_size)
    cropped_data = center_crop(grid.data, crop_size) if data is None else data
    return AsciiGrid(
        data=cropped_data,
        ncols=crop_size,
        nrows=crop_size,
        xllcorner=grid.xllcorner + col0 * grid.cellsize,
        yllcorner=grid.yllcorner + bottom_removed * grid.cellsize,
        cellsize=grid.cellsize,
        nodata=grid.nodata,
    )

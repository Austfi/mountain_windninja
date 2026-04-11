from pathlib import Path

import pytest

from scripts.build_lcp_from_geotiff import (
    build_translate_command,
    ensure_output_path,
    output_prj_path,
    validate_band_count,
)


def test_build_translate_command_uses_first_eight_bands(tmp_path):
    input_tif = tmp_path / "landscape.tif"
    output_lcp = tmp_path / "summit_county_surface.lcp"

    command = build_translate_command(input_tif, output_lcp, "Summit County")

    assert command[:3] == ["gdal_translate", "-of", "LCP"]
    assert command.count("-b") == 8
    assert command[-2:] == [str(input_tif), str(output_lcp)]
    assert "DESCRIPTION=Summit County" in command


def test_output_prj_path_tracks_lcp_suffix():
    output_lcp = Path("/tmp/summit_county_surface.lcp")
    assert output_prj_path(output_lcp) == Path("/tmp/summit_county_surface.prj")


def test_validate_band_count_requires_eight_bands():
    with pytest.raises(ValueError):
        validate_band_count(7)

    validate_band_count(8)


def test_ensure_output_path_requires_force_to_overwrite(tmp_path):
    output_lcp = tmp_path / "summit_county_surface.lcp"
    output_lcp.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError):
        ensure_output_path(output_lcp, force=False)

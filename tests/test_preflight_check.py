from pathlib import Path

from scripts.preflight_check import _lcp_prj_status


def test_lcp_prj_status_requires_prj_sidecar(tmp_path):
    lcp_path = tmp_path / "summit_county_surface.lcp"
    lcp_path.write_bytes(b"placeholder")

    passed, detail = _lcp_prj_status(lcp_path)

    assert passed is False
    assert str(lcp_path.with_suffix(".prj")) in detail


def test_lcp_prj_status_passes_when_prj_exists(tmp_path):
    lcp_path = tmp_path / "summit_county_surface.lcp"
    prj_path = tmp_path / "summit_county_surface.prj"
    lcp_path.write_bytes(b"placeholder")
    prj_path.write_text("PROJCS[\"Test\"]", encoding="utf-8")

    passed, detail = _lcp_prj_status(lcp_path)

    assert passed is True
    assert detail == str(prj_path)

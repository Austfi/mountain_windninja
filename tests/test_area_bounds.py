from __future__ import annotations

import pytest

from scripts import area_bounds


def test_bbox_from_center_builds_square_extent():
    bounds = area_bounds.bbox_from_center("39.60", "-106.08", "12")

    assert bounds.north > 39.60
    assert bounds.south < 39.60
    assert bounds.east > -106.08
    assert bounds.west < -106.08
    assert bounds.north == pytest.approx(39.65396, rel=1e-4)
    assert bounds.east == pytest.approx(-106.01008, rel=1e-4)


def test_bbox_from_center_rejects_large_beginner_domain():
    with pytest.raises(area_bounds.AreaBoundsError, match="limited to 50"):
        area_bounds.bbox_from_center("39.60", "-106.08", "75")


def test_bbox_from_kml_reads_polygon(tmp_path):
    kml = tmp_path / "area.kml"
    kml.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Placemark>
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>
          <coordinates>
            -106.15,39.55,0 -106.00,39.55,0 -106.00,39.65,0 -106.15,39.65,0
          </coordinates>
        </LinearRing>
      </outerBoundaryIs>
    </Polygon>
  </Placemark>
</kml>
""",
        encoding="utf-8",
    )

    bounds = area_bounds.bbox_from_kml(kml)

    assert bounds.north == 39.65
    assert bounds.east == -106.00
    assert bounds.south == 39.55
    assert bounds.west == -106.15


def test_bbox_from_kml_rejects_point_without_padding(tmp_path):
    kml = tmp_path / "point.kml"
    kml.write_text(
        """<kml xmlns="http://www.opengis.net/kml/2.2">
  <Placemark><Point><coordinates>-106.08,39.60,0</coordinates></Point></Placemark>
</kml>
""",
        encoding="utf-8",
    )

    with pytest.raises(area_bounds.AreaBoundsError, match="no north/south extent"):
        area_bounds.bbox_from_kml(kml)


def test_bbox_from_kml_point_with_padding(tmp_path):
    kml = tmp_path / "point.kml"
    kml.write_text(
        """<kml xmlns="http://www.opengis.net/kml/2.2">
  <Placemark><Point><coordinates>-106.08,39.60,0</coordinates></Point></Placemark>
</kml>
""",
        encoding="utf-8",
    )

    bounds = area_bounds.bbox_from_kml(kml, padding_km=1)

    assert bounds.north > 39.60
    assert bounds.south < 39.60
    assert bounds.east > -106.08
    assert bounds.west < -106.08

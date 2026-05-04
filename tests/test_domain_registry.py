from __future__ import annotations

import json

import pytest

from scripts import domain_registry


def test_validate_bbox_rejects_swapped_north_south():
    with pytest.raises(domain_registry.DomainRegistryError, match="North latitude"):
        domain_registry.validate_bbox("39.5", "-106.0", "39.6", "-106.2")


def test_validate_bbox_rejects_swapped_east_west():
    with pytest.raises(domain_registry.DomainRegistryError, match="East longitude"):
        domain_registry.validate_bbox("39.6", "-106.2", "39.5", "-106.0")


def test_validate_bbox_rejects_non_numeric():
    with pytest.raises(domain_registry.DomainRegistryError, match="numeric"):
        domain_registry.validate_bbox("north", "-106.0", "39.5", "-106.2")


def test_validate_domain_key_rejects_bad_characters():
    with pytest.raises(domain_registry.DomainRegistryError, match="Domain key"):
        domain_registry.validate_domain_key("bad/key")


def test_default_terrain_output_path_uses_source_suffix():
    assert (
        domain_registry.default_terrain_output_path("my_area", "us")
        == "static_data/my_area.tif"
    )
    assert (
        domain_registry.default_terrain_output_path("my_area", "srtm")
        == "static_data/my_area.tif"
    )
    assert (
        domain_registry.default_terrain_output_path("my_area", "gmted")
        == "static_data/my_area.tif"
    )
    assert (
        domain_registry.default_terrain_output_path("my_area", "lcp")
        == "static_data/my_area.lcp"
    )


def test_default_terrain_output_path_uses_static_data_root(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    runtime_env_path = config_dir / "runtime.env"
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=terrain_cache\n", encoding="utf-8")

    assert (
        domain_registry.default_terrain_output_path(
            "my_area",
            "us",
            base_dir=tmp_path,
            runtime_env_path=runtime_env_path,
        )
        == "terrain_cache/my_area.tif"
    )
    assert (
        domain_registry.default_terrain_output_path(
            "my_area",
            "lcp",
            base_dir=tmp_path,
            runtime_env_path=runtime_env_path,
        )
        == "terrain_cache/my_area.lcp"
    )


def test_default_terrain_output_path_rejects_unknown_source():
    with pytest.raises(domain_registry.DomainRegistryError, match="Terrain source"):
        domain_registry.default_terrain_output_path("my_area", "bogus")


def test_register_domain_adds_domain_and_sets_default(tmp_path):
    base_dir = tmp_path
    config_dir = base_dir / "config"
    static_dir = base_dir / "static_data"
    config_dir.mkdir()
    static_dir.mkdir()
    domains_path = config_dir / "domains.json"
    runtime_env_path = config_dir / "runtime.env"
    terrain = static_dir / "my_area.tif"
    terrain.write_text("dem", encoding="utf-8")
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=static_data\nMWN_DOMAIN_ID=old\n", encoding="utf-8")
    domains_path.write_text(json.dumps({"default_domain": "old", "domains": {}}), encoding="utf-8")

    entry = domain_registry.register_domain(
        "my_area",
        terrain,
        label="My Area",
        set_default=True,
        base_dir=base_dir,
        domains_path=domains_path,
        runtime_env_path=runtime_env_path,
    )

    payload = json.loads(domains_path.read_text(encoding="utf-8"))
    assert entry == {
        "label": "My Area",
        "template": "config/template.cfg",
        "elevation_file": "my_area.tif",
    }
    assert payload["default_domain"] == "my_area"
    assert payload["domains"]["my_area"] == entry
    assert "MWN_DOMAIN_ID=my_area" in runtime_env_path.read_text(encoding="utf-8")


def test_register_domain_without_set_default_preserves_default(tmp_path):
    base_dir = tmp_path
    config_dir = base_dir / "config"
    static_dir = base_dir / "static_data"
    config_dir.mkdir()
    static_dir.mkdir()
    domains_path = config_dir / "domains.json"
    runtime_env_path = config_dir / "runtime.env"
    terrain = static_dir / "my_area.tif"
    terrain.write_text("dem", encoding="utf-8")
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=static_data\n", encoding="utf-8")
    domains_path.write_text(
        json.dumps({"default_domain": "old", "domains": {}}),
        encoding="utf-8",
    )

    domain_registry.register_domain(
        "my_area",
        terrain,
        base_dir=base_dir,
        domains_path=domains_path,
        runtime_env_path=runtime_env_path,
    )

    payload = json.loads(domains_path.read_text(encoding="utf-8"))
    assert payload["default_domain"] == "old"
    assert "MWN_DOMAIN_ID=my_area" not in runtime_env_path.read_text(encoding="utf-8")


def test_register_domain_updates_existing_domain_without_replacing_template(tmp_path):
    base_dir = tmp_path
    config_dir = base_dir / "config"
    static_dir = base_dir / "static_data"
    config_dir.mkdir()
    static_dir.mkdir()
    domains_path = config_dir / "domains.json"
    runtime_env_path = config_dir / "runtime.env"
    terrain = static_dir / "new_area.tif"
    terrain.write_text("dem", encoding="utf-8")
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=static_data\n", encoding="utf-8")
    domains_path.write_text(
        json.dumps({
            "default_domain": "my_area",
            "domains": {
                "my_area": {
                    "label": "Existing",
                    "template": "config/custom.cfg",
                    "elevation_file": "old_area.tif",
                }
            },
        }),
        encoding="utf-8",
    )

    domain_registry.register_domain(
        "my_area",
        "static_data/new_area.tif",
        base_dir=base_dir,
        domains_path=domains_path,
        runtime_env_path=runtime_env_path,
    )

    payload = json.loads(domains_path.read_text(encoding="utf-8"))
    assert payload["domains"]["my_area"] == {
        "label": "Existing",
        "template": "config/custom.cfg",
        "elevation_file": "new_area.tif",
    }


def test_register_domain_requires_static_data_path(tmp_path):
    base_dir = tmp_path
    config_dir = base_dir / "config"
    config_dir.mkdir()
    outside = base_dir / "outside.tif"
    outside.write_text("dem", encoding="utf-8")
    runtime_env_path = config_dir / "runtime.env"
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=static_data\n", encoding="utf-8")

    with pytest.raises(domain_registry.DomainRegistryError, match="inside"):
        domain_registry.register_domain(
            "bad",
            outside,
            base_dir=base_dir,
            domains_path=config_dir / "domains.json",
            runtime_env_path=runtime_env_path,
        )


def test_load_save_and_upsert_domain_wrappers(tmp_path):
    base_dir = tmp_path
    config_dir = base_dir / "config"
    static_dir = base_dir / "static_data"
    config_dir.mkdir()
    static_dir.mkdir()
    domains_path = config_dir / "domains.json"
    runtime_env_path = config_dir / "runtime.env"
    terrain = static_dir / "new_area.tif"
    terrain.write_text("dem", encoding="utf-8")
    runtime_env_path.write_text("MWN_STATIC_DATA_ROOT=static_data\n", encoding="utf-8")

    domain_registry.save_domains(
        {"default_domain": None, "domains": {}},
        path=domains_path,
    )
    entry = domain_registry.upsert_domain(
        "new_area",
        "New Area",
        terrain,
        template="config/custom.cfg",
        set_default=True,
        base_dir=base_dir,
        domains_path=domains_path,
        runtime_env_path=runtime_env_path,
    )
    payload = domain_registry.load_domains(domains_path)

    assert entry == {
        "label": "New Area",
        "template": "config/custom.cfg",
        "elevation_file": "new_area.tif",
    }
    assert payload["default_domain"] == "new_area"
    assert payload["domains"]["new_area"] == entry

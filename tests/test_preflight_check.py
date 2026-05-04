import scripts.preflight_check as preflight_check
from scripts.config_loader import DomainConfig
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


def test_build_report_uses_requested_domain(tmp_path, monkeypatch):
    default_template = tmp_path / "default.cfg"
    requested_template = tmp_path / "requested.cfg"
    default_surface = tmp_path / "default.tif"
    requested_surface = tmp_path / "requested.tif"
    for path in (default_template, requested_template, default_surface, requested_surface):
        path.write_text("placeholder", encoding="utf-8")

    domains = {
        "default": DomainConfig(
            key="default",
            label="Default",
            template_path=default_template,
            elevation_file=default_surface,
        ),
        "requested": DomainConfig(
            key="requested",
            label="Requested",
            template_path=requested_template,
            elevation_file=requested_surface,
        ),
    }
    calls = []

    def fake_get_domain_config(domain_key=None):
        calls.append(domain_key)
        return domains[domain_key or "default"]

    monkeypatch.setattr(preflight_check.config_loader, "get_domain_config", fake_get_domain_config)
    monkeypatch.setattr(preflight_check.config_loader, "RUNTIME_DIR", tmp_path / "runtime")
    monkeypatch.setattr(preflight_check.config_loader, "STATIC_DATA_DIR", tmp_path / "static")
    monkeypatch.setattr(preflight_check.config_loader, "WINDNINJA_CLI", str(tmp_path / "WindNinja_cli"))
    monkeypatch.setattr(preflight_check.config_loader, "OPENFOAM_BASHRC", str(tmp_path / "bashrc"))
    monkeypatch.setattr(preflight_check.config_loader, "GCS_UPLOAD_ENABLED", False)
    monkeypatch.setattr(preflight_check.shutil, "which", lambda _command: None)

    report = preflight_check.build_report(check_gcs=False, domain_key="requested")

    assert calls == ["requested"]
    assert report["domain_id"] == "requested"
    details = {check["name"]: check["detail"] for check in report["checks"]}
    assert details["domain_template"] == str(requested_template)
    assert details["domain_surface_file"] == str(requested_surface)

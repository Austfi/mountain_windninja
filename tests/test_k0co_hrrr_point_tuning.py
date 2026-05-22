import pytest

from scripts import k0co_hrrr_point_tuning as tuning


def _record() -> dict:
    return {
        "sample_time_utc": "2026-01-01T00:00:00Z",
        "obs_speed": 0.0,
        "obs_dir_deg": 0.0,
        "u10": 1.0,
        "v10": 0.0,
        "u80": 5.0,
        "v80": 0.0,
        "hrrr_surface_hgt_m": 1000.0,
    }


def test_settings_include_requested_hrrr_only_variants():
    names = {setting.name for setting in tuning.settings()}

    assert "HRRR_80m" in names
    assert "blend_scale_300m_no_cap" in names
    assert "blend_scale_300m_cap_10_80_low_0.75_high_1.10" in names
    assert "blend_scale_300m_low_0.75_high_1.35_slack_2mph" in names


def test_raw_80m_uses_full_80m_vector_without_cap():
    result = tuning.evaluate_setting(
        _record(),
        tuning.Setting("HRRR_80m", "raw_hrrr_80m", cap_mode="none", fixed_weight=1.0),
        gmted_elevation_m=1300.0,
    )

    assert result["weight"] == 1.0
    assert result["cap"] == ""
    assert result["speed_mph"] == pytest.approx(11.184681)


def test_no_cap_blend_keeps_full_adjusted_speed():
    result = tuning.evaluate_setting(
        _record(),
        tuning.Setting(
            "blend_no_cap",
            "elevation_blend",
            cap_mode="none",
            blend_scale_m=300.0,
        ),
        gmted_elevation_m=1300.0,
    )

    assert result["weight"] == 1.0
    assert result["cap"] == ""
    assert result["speed_mph"] == pytest.approx(11.184681)


def test_current_raw_10m_cap_can_limit_80m_shear():
    result = tuning.evaluate_setting(
        _record(),
        tuning.Setting(
            "current_cap",
            "elevation_blend",
            cap_mode="raw_10m",
            low_cap=0.75,
            high_cap=1.35,
            blend_scale_m=300.0,
        ),
        gmted_elevation_m=1300.0,
    )

    assert result["cap"] == "high"
    assert result["speed_mph"] == pytest.approx(3.019864)


def test_cap_relative_to_both_levels_allows_valid_80m_shear():
    result = tuning.evaluate_setting(
        _record(),
        tuning.Setting(
            "level_cap",
            "elevation_blend",
            cap_mode="levels_10_80",
            low_cap=0.75,
            high_cap=1.10,
            blend_scale_m=300.0,
        ),
        gmted_elevation_m=1300.0,
    )

    assert result["cap"] == ""
    assert result["speed_mph"] == pytest.approx(11.184681)


def test_raw_10m_cap_with_absolute_slack_relaxes_high_cap():
    result = tuning.evaluate_setting(
        _record(),
        tuning.Setting(
            "slack_cap",
            "elevation_blend",
            cap_mode="raw_10m_slack",
            low_cap=0.75,
            high_cap=1.35,
            blend_scale_m=300.0,
            absolute_slack_mph=2.0,
        ),
        gmted_elevation_m=1300.0,
    )

    assert result["cap"] == "high"
    assert result["speed_mph"] == pytest.approx(5.019864)

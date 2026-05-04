"""WindNinja .cfg generation helpers."""
from __future__ import annotations

import datetime
import os
import re
from pathlib import Path

try:
    from . import archive_manager, config_loader, utils
except ImportError:
    import archive_manager
    import config_loader
    import utils

logger = utils.setup_logging(__name__)


def generate_config(date_str, start_time, stop_time, domain_config,
                    wx_model_type_override=None, surface_vegetation=None,
                    sub_dir=None, output_wind_height=10.0,
                    input_points_file=None, output_points_file=None,
                    run_type="forecast"):
    """Read a template .cfg, fill placeholders, write a ready-to-run config."""
    run_output_dir = sub_dir or os.path.join(config_loader.TEMP_DIR, date_str)
    utils.ensure_dir(run_output_dir)

    with open(str(domain_config.template_path), "r") as f:
        template = f.read()

    duration = max(1, int((stop_time - start_time).total_seconds() / 3600))

    filled = template.format(
        start_year=start_time.year, start_month=start_time.month,
        start_day=start_time.day, start_hour=start_time.hour,
        start_minute=start_time.minute,
        stop_year=stop_time.year, stop_month=stop_time.month,
        stop_day=stop_time.day, stop_hour=stop_time.hour,
        stop_minute=stop_time.minute,
        forecast_duration=duration,
        elevation_file=domain_config.elevation_file.as_posix(),
        output_wind_height=output_wind_height,
    )

    lines = filled.split("\n")
    out_lines = []
    found_vegetation = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("output_path"):
            continue

        if stripped.startswith("input_points_file"):
            continue

        if stripped.startswith("output_points_file"):
            continue

        if run_type == "reanalysis" and stripped.startswith("forecast_duration"):
            continue

        if wx_model_type_override and stripped.startswith("wx_model_type"):
            out_lines.append(f"wx_model_type = {wx_model_type_override}")
            continue

        if surface_vegetation and stripped.startswith("vegetation"):
            out_lines.append(f"vegetation = {surface_vegetation}")
            found_vegetation = True
            continue

        out_lines.append(line)

    if (surface_vegetation and surface_vegetation != "none"
            and domain_config.elevation_file.suffix.lower() != ".lcp"
            and not found_vegetation):
        out_lines.append(f"vegetation = {surface_vegetation}")

    if input_points_file:
        out_lines.append(f"input_points_file = {Path(input_points_file).as_posix()}")
        if not output_points_file:
            output_points_file = os.path.join(
                run_output_dir, f"{domain_config.key}_sample_points.csv",
            )
        output_points_path = Path(output_points_file)
        utils.ensure_dir(str(output_points_path.parent))
        out_lines.append(f"output_points_file = {output_points_path.as_posix()}")

    out_lines.append(f"output_path = {run_output_dir}")

    config_path = os.path.join(
        run_output_dir,
        f"{domain_config.key}_{start_time.strftime('%Y%m%d_%H%M')}.cfg",
    )
    with open(config_path, "w") as f:
        f.write("\n".join(out_lines))

    return config_path, run_output_dir


def _read_template_num_threads(template_path, fallback=4):
    override = os.getenv("MWN_NUM_THREADS")
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            logger.warning(f"Ignoring invalid MWN_NUM_THREADS={override!r}; using template value.")

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            for line in f:
                match = re.match(r"^\s*num_threads\s*=\s*(\d+)\s*$", line)
                if match:
                    return max(1, int(match.group(1)))
    except OSError as exc:
        logger.warning(f"Could not read num_threads from template {template_path}: {exc}")
    return fallback


def _read_template_key_values(template_path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except OSError as exc:
        logger.warning(f"Could not read template settings from {template_path}: {exc}")
    return values


def _template_value(values: dict[str, str], key: str, fallback: str) -> str:
    value = values.get(key, fallback)
    if "{" in value or "}" in value:
        return fallback
    return value


def _template_bool(values: dict[str, str], key: str, fallback: bool) -> bool:
    raw_value = _template_value(values, key, "true" if fallback else "false")
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return fallback


def template_momentum_enabled(template_path, fallback: bool = True) -> bool:
    """Return whether a WindNinja template resolves to momentum solver mode."""
    return _template_bool(_read_template_key_values(template_path), "momentum_flag", fallback)


def generate_domain_average_config(domain_config, wind_speed, wind_direction,
                                   speed_units="mph", surface_vegetation=None,
                                   sub_dir=None, output_wind_height=10.0,
                                   input_points_file=None, output_points_file=None):
    """Generate a domain-average config (single uniform wind, no wx download)."""
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    date_str = now_utc.strftime("%Y%m%d_%H%M")
    run_output_dir = sub_dir or os.path.join(
        config_loader.TEMP_DIR,
        archive_manager.build_domain_average_output_dir_name(domain_config.key, date_str),
    )
    utils.ensure_dir(run_output_dir)

    vegetation = surface_vegetation or config_loader.SURFACE_VEGETATION
    use_lcp = domain_config.elevation_file.suffix.lower() == ".lcp"
    template_values = _read_template_key_values(domain_config.template_path)
    num_threads = _read_template_num_threads(domain_config.template_path)

    lines = [
        f"num_threads = {num_threads}",
        f"elevation_file = {domain_config.elevation_file.as_posix()}",
        "",
        "initialization_method = domainAverageInitialization",
        f"input_speed = {wind_speed}",
        f"input_speed_units = {speed_units}",
        f"input_direction = {wind_direction}",
        "input_wind_height = 10.0",
        "units_input_wind_height = m",
        "",
    ]

    if not use_lcp and vegetation and vegetation != "none":
        lines.append(f"vegetation = {vegetation}")

    lines += [
        "diurnal_winds = false",
        "",
        f"year  = {now_utc.year}",
        f"month = {now_utc.month}",
        f"day   = {now_utc.day}",
        f"hour  = {now_utc.hour}",
        f"minute = {now_utc.minute}",
        "time_zone = UTC",
        "",
        "mesh_resolution = 80.0",
        "units_mesh_resolution = m",
        "",
        f"momentum_flag = {_template_value(template_values, 'momentum_flag', 'true')}",
        "number_of_iterations = 300",
        "",
        f"output_wind_height = {output_wind_height}",
        "units_output_wind_height = m",
        f"output_speed_units = {speed_units}",
        "",
        "write_goog_output = true",
        "goog_out_use_consistent_color_scale = false",
        "units_goog_out_resolution = m",
        "",
        "write_ascii_output = true",
        "ascii_out_resolution = -1",
        "units_ascii_out_resolution = m",
        "",
    ]

    if input_points_file:
        lines.append(f"input_points_file = {Path(input_points_file).as_posix()}")
        if not output_points_file:
            output_points_file = os.path.join(
                run_output_dir, f"{domain_config.key}_sample_points.csv",
            )
        output_points_path = Path(output_points_file)
        utils.ensure_dir(str(output_points_path.parent))
        lines.append(f"output_points_file = {output_points_path.as_posix()}")

    lines += [
        "",
        f"output_path = {run_output_dir}",
    ]

    config_path = os.path.join(
        run_output_dir, f"{domain_config.key}_domavg_{date_str}.cfg",
    )
    with open(config_path, "w") as f:
        f.write("\n".join(lines))

    return config_path, run_output_dir


def generate_gridded_config(
    domain_config,
    speed_grid,
    direction_grid,
    run_time,
    *,
    label=None,
    surface_vegetation=None,
    sub_dir=None,
    output_wind_height=10.0,
):
    """Generate a one-timestep griddedInitialization config."""
    start_label = run_time.strftime("%Y%m%d_%H%M")
    run_output_dir = sub_dir or os.path.join(
        config_loader.TEMP_DIR,
        archive_manager.build_grid_output_dir_name(domain_config.key, run_time, label),
    )
    utils.ensure_dir(run_output_dir)

    vegetation = surface_vegetation or config_loader.SURFACE_VEGETATION
    use_lcp = domain_config.elevation_file.suffix.lower() == ".lcp"
    template_values = _read_template_key_values(domain_config.template_path)
    num_threads = _read_template_num_threads(domain_config.template_path)

    lines = [
        f"num_threads = {num_threads}",
        f"elevation_file = {domain_config.elevation_file.as_posix()}",
        "",
        "initialization_method = griddedInitialization",
        f"input_speed_grid = {Path(speed_grid).as_posix()}",
        f"input_dir_grid = {Path(direction_grid).as_posix()}",
        "input_speed_units = mps",
        "input_wind_height = 10.0",
        "units_input_wind_height = m",
        "",
    ]

    if not use_lcp and vegetation and vegetation != "none":
        lines.append(f"vegetation = {vegetation}")
        lines.append("")

    lines += [
        "diurnal_winds = false",
        "",
        f"year  = {run_time.year}",
        f"month = {run_time.month}",
        f"day   = {run_time.day}",
        f"hour  = {run_time.hour}",
        f"minute = {run_time.minute}",
        "time_zone = UTC",
        "",
        f"mesh_resolution = {_template_value(template_values, 'mesh_resolution', '80.0')}",
        f"units_mesh_resolution = {_template_value(template_values, 'units_mesh_resolution', 'm')}",
        "",
        f"momentum_flag = {_template_value(template_values, 'momentum_flag', 'true')}",
        "number_of_iterations = "
        f"{_template_value(template_values, 'number_of_iterations', '300')}",
        "",
        f"output_wind_height = {output_wind_height}",
        "units_output_wind_height = m",
        "output_speed_units = "
        f"{_template_value(template_values, 'output_speed_units', 'mph')}",
        "",
        "write_goog_output = "
        f"{_template_value(template_values, 'write_goog_output', 'true')}",
        "goog_out_use_consistent_color_scale = "
        f"{_template_value(template_values, 'goog_out_use_consistent_color_scale', 'false')}",
        "units_goog_out_resolution = "
        f"{_template_value(template_values, 'units_goog_out_resolution', 'm')}",
        "",
        "write_ascii_output = "
        f"{_template_value(template_values, 'write_ascii_output', 'true')}",
        "ascii_out_resolution = "
        f"{_template_value(template_values, 'ascii_out_resolution', '-1')}",
        "units_ascii_out_resolution = "
        f"{_template_value(template_values, 'units_ascii_out_resolution', 'm')}",
        "",
    ]

    for key in (
        "write_shapefile_output",
        "write_pdf_output",
        "write_vtk_output",
        "write_farsite_atm",
    ):
        if key in template_values:
            lines.append(f"{key} = {_template_value(template_values, key, 'false')}")

    lines += [
        "",
        f"output_path = {run_output_dir}",
    ]

    config_path = os.path.join(
        run_output_dir, f"{domain_config.key}_grid_{start_label}.cfg",
    )
    with open(config_path, "w") as f:
        f.write("\n".join(lines))

    return config_path, run_output_dir

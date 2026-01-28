from __future__ import annotations

import re
from pathlib import Path

from .config import ClientConfig, load_config


def discover_configs(config_dir: Path) -> list[tuple[str, Path]]:
    """Find all FIM_config_[0-9]*.json files, return sorted by ID."""
    pattern = re.compile(r"^FIM_config_(\d+)\.json$")
    configs: list[tuple[str, Path]] = []
    if not config_dir.is_dir():
        return configs
    for entry in config_dir.iterdir():
        if not entry.is_file():
            continue
        match = pattern.match(entry.name)
        if match:
            config_id = match.group(1)
            configs.append((config_id, entry))
    configs.sort(key=lambda x: int(x[0]))
    return configs


def derive_tag_from_config(config_path: Path, config: ClientConfig) -> str:
    """Return config.tag if valid, else derive from filename."""
    if config.tag and config.tag.strip():
        return config.tag.strip()
    stem = config_path.stem
    if stem.startswith("FIM_config_"):
        return stem.replace("FIM_config_", "config_")
    return stem


def derive_paths_from_tag(base_dir: Path, tag: str) -> dict[str, Path]:
    """Return {state_path, lock_path, log_path} using standard layout."""
    return {
        "state_path": base_dir / "data" / "state" / f"{tag}.json",
        "lock_path": base_dir / "data" / "locks" / f"{tag}.lock",
        "log_path": base_dir / "log" / f"client_{tag}.log",
    }


def validate_scan_path_disjoint(configs: list[tuple[str, ClientConfig]]) -> list[str]:
    """Warn if any config's scan_paths overlap with another's."""
    warnings: list[str] = []
    resolved_paths: list[tuple[str, list[Path]]] = []

    for tag, config in configs:
        paths = []
        for sp in config.scan_paths:
            try:
                resolved = Path(sp).resolve()
                paths.append(resolved)
            except Exception:
                paths.append(Path(sp))
        resolved_paths.append((tag, paths))

    for i, (tag_a, paths_a) in enumerate(resolved_paths):
        for j, (tag_b, paths_b) in enumerate(resolved_paths):
            if i >= j:
                continue
            for pa in paths_a:
                for pb in paths_b:
                    if _paths_overlap(pa, pb):
                        warnings.append(
                            f"Overlap detected: [{tag_a}] {pa} overlaps with [{tag_b}] {pb}"
                        )
    return warnings


def _paths_overlap(a: Path, b: Path) -> bool:
    """Check if two paths overlap (one is prefix of the other)."""
    try:
        a.relative_to(b)
        return True
    except ValueError:
        pass
    try:
        b.relative_to(a)
        return True
    except ValueError:
        pass
    return a == b


def parse_schedule_key_to_minutes(key: str) -> int | None:
    """Convert schedule key like 'Mon0910' to minutes-since-week-start (Mon 00:00 = 0)."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if len(key) < 7:
        return None
    day_part = key[:3]
    time_part = key[3:]
    if day_part not in days:
        return None
    if len(time_part) != 4 or not time_part.isdigit():
        return None
    hour = int(time_part[:2])
    minute = int(time_part[2:])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    day_index = days.index(day_part)
    return day_index * 24 * 60 + hour * 60 + minute


def validate_schedule_spacing(
    configs: list[tuple[str, ClientConfig]], min_gap_minutes: int = 5
) -> list[str]:
    """Warn if any two schedules across configs are < min_gap_minutes apart."""
    warnings: list[str] = []
    schedule_entries: list[tuple[str, str, int]] = []

    for tag, config in configs:
        for key in config.schedule_quota_gb.keys():
            minutes = parse_schedule_key_to_minutes(key)
            if minutes is not None:
                schedule_entries.append((tag, key, minutes))

    schedule_entries.sort(key=lambda x: x[2])

    week_minutes = 7 * 24 * 60

    for i, (tag_a, key_a, min_a) in enumerate(schedule_entries):
        for j, (tag_b, key_b, min_b) in enumerate(schedule_entries):
            if i >= j:
                continue
            if tag_a == tag_b:
                continue
            diff = abs(min_b - min_a)
            wrap_diff = week_minutes - diff
            actual_gap = min(diff, wrap_diff)
            if actual_gap < min_gap_minutes:
                warnings.append(
                    f"Schedule conflict: [{tag_a}] {key_a} and [{tag_b}] {key_b} "
                    f"are only {actual_gap} minutes apart (min: {min_gap_minutes})"
                )
    return warnings

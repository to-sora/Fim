from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .config import load_config


DAY_INDEX = {
    "Mon": 0,
    "Tue": 1,
    "Wed": 2,
    "Thu": 3,
    "Fri": 4,
    "Sat": 5,
    "Sun": 6,
}

SCHEDULE_RE = re.compile(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)(\d{2})(\d{2})$")


@dataclass(frozen=True)
class ScheduleEntry:
    config_path: str
    tag: str
    key: str
    day_index: int
    minutes: int


def discover_config_paths(config_dir: str | Path, pattern: str) -> list[Path]:
    root = Path(config_dir)
    paths = sorted(root.glob(pattern))
    return [p for p in paths if p.is_file()]


def parse_schedule_key(key: str) -> tuple[int, int] | None:
    match = SCHEDULE_RE.match(key)
    if not match:
        return None
    day, hh_raw, mm_raw = match.groups()
    hh = int(hh_raw)
    mm = int(mm_raw)
    if hh > 23 or mm > 59:
        return None
    return DAY_INDEX[day], hh * 60 + mm


def collect_schedule_entries(config_paths: list[Path]) -> tuple[list[ScheduleEntry], list[dict[str, str]]]:
    entries: list[ScheduleEntry] = []
    invalid: list[dict[str, str]] = []
    for path in config_paths:
        cfg = load_config(path)
        for key, quota in cfg.schedule_quota_gb.items():
            if quota is None or int(quota) <= 0:
                continue
            parsed = parse_schedule_key(key)
            if parsed is None:
                invalid.append({"config": str(path), "key": key})
                continue
            day_index, minutes = parsed
            entries.append(
                ScheduleEntry(
                    config_path=str(path),
                    tag=cfg.tag or "",
                    key=key,
                    day_index=day_index,
                    minutes=minutes,
                )
            )
    return entries, invalid


def find_schedule_conflicts(
    entries: list[ScheduleEntry], min_gap_min: int
) -> list[dict[str, object]]:
    conflicts: list[dict[str, object]] = []
    for i, a in enumerate(entries):
        for b in entries[i + 1 :]:
            if a.config_path == b.config_path:
                continue
            if a.day_index != b.day_index:
                continue
            gap = abs(a.minutes - b.minutes)
            if gap < min_gap_min:
                conflicts.append(
                    {
                        "config_a": a.config_path,
                        "config_b": b.config_path,
                        "tag_a": a.tag,
                        "tag_b": b.tag,
                        "key_a": a.key,
                        "key_b": b.key,
                        "gap_min": gap,
                    }
                )
    return conflicts


def verify_config_schedules(
    config_paths: list[Path], *, min_gap_min: int
) -> dict[str, object]:
    entries, invalid = collect_schedule_entries(config_paths)
    conflicts = find_schedule_conflicts(entries, min_gap_min)
    configs = []
    for path in config_paths:
        cfg = load_config(path)
        configs.append(
            {
                "path": str(path),
                "tag": cfg.tag or "",
                "schedule_keys": sorted(
                    [k for k, v in cfg.schedule_quota_gb.items() if v is not None and int(v) > 0]
                ),
            }
        )
    status = "ok" if not conflicts and not invalid else "conflict"
    return {
        "status": status,
        "min_gap_min": int(min_gap_min),
        "configs": configs,
        "invalid_keys": invalid,
        "conflicts": conflicts,
    }

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable


@dataclass(frozen=True)
class ShaSegment:
    machine_name: str
    base_urn: str
    start_date: str
    end_date: str
    segment_index: int = 0  # Distinguishes separate appearances at same location


_SAFE_NODE_RE = re.compile(r"[^a-zA-Z0-9_]+")


def _base_urn(urn: str) -> str:
    parts = urn.split(":")
    if len(parts) < 2:
        return urn
    return ":".join(parts[:-1])


def _scan_date_from_urn_or_ts(urn: str, scan_ts: str | None) -> str:
    parts = urn.split(":")
    if len(parts) >= 2:
        tail = parts[-1]
        try:
            date.fromisoformat(tail)
            return tail
        except ValueError:
            pass
    if scan_ts:
        try:
            return datetime.fromisoformat(scan_ts).date().isoformat()
        except ValueError:
            pass
    # Fallback: unknown scan_date; keep stable ordering
    return "1970-01-01"


def _split_dates_by_gap(dates: list[str], gap_days: int = 30) -> list[list[str]]:
    """Split a sorted list of dates into groups separated by gaps > gap_days."""
    if not dates:
        return []
    groups: list[list[str]] = [[dates[0]]]
    for i in range(1, len(dates)):
        prev = date.fromisoformat(dates[i - 1])
        curr = date.fromisoformat(dates[i])
        if (curr - prev).days > gap_days:
            groups.append([dates[i]])
        else:
            groups[-1].append(dates[i])
    return groups


def fetch_segments_for_sha256(
    conn: sqlite3.Connection, *, sha256: str, limit: int = 20000, gap_days: int = 30
) -> list[ShaSegment]:
    """Fetch segments for a SHA256, splitting into separate nodes when gaps > gap_days.

    This ensures that if a file disappears from a location and reappears later,
    it creates separate nodes rather than one continuous span.
    """
    limit = max(1, min(int(limit), 200000))
    rows = conn.execute(
        """
        SELECT machine_name, urn, scan_ts
        FROM file_record
        WHERE sha256 = ?
        ORDER BY machine_name ASC, scan_ts ASC, id ASC
        LIMIT ?
        """,
        (sha256, limit),
    ).fetchall()

    grouped: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        machine = str(r["machine_name"] or "")
        urn = str(r["urn"] or "")
        scan_ts = str(r["scan_ts"] or "")
        base = _base_urn(urn)
        d = _scan_date_from_urn_or_ts(urn, scan_ts)
        grouped.setdefault((machine, base), []).append(d)

    segments: list[ShaSegment] = []
    for (machine, base), dates in grouped.items():
        unique_sorted = sorted(set(dates))
        if not unique_sorted:
            continue
        # Split into separate segments when there are gaps
        date_groups = _split_dates_by_gap(unique_sorted, gap_days)
        for idx, group in enumerate(date_groups):
            segments.append(
                ShaSegment(
                    machine_name=machine,
                    base_urn=base,
                    start_date=group[0],
                    end_date=group[-1],
                    segment_index=idx,
                )
            )

    segments.sort(key=lambda s: (s.machine_name, s.start_date, s.base_urn, s.segment_index))
    return segments


def render_ascii_chain(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = []
    for machine, segs in sorted(by_machine.items()):
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn, s.segment_index))

        def _disp(s: ShaSegment) -> str:
            prefix = f"{machine}:"
            path = s.base_urn[len(prefix):] if s.base_urn.startswith(prefix) else s.base_urn
            # Add segment marker if file reappeared at same location
            seg_marker = f"#{s.segment_index + 1}" if s.segment_index > 0 else ""
            return f"{path}{seg_marker}"

        parts = [
            f"{{{_disp(s)} {s.start_date}..{s.end_date}}}" for s in segs_sorted
        ]
        chain = " -> ".join(parts) if parts else "(no data)"
        lines.append(f"{machine} {chain}")
    return "\n".join(lines)


def _node_id(machine: str, base_urn: str, segment_index: int = 0) -> str:
    """Generate safe node ID including segment_index for uniqueness."""
    suffix = f"_seg{segment_index}" if segment_index > 0 else ""
    raw = f"{machine}_{base_urn}{suffix}"
    safe = _SAFE_NODE_RE.sub("_", raw)
    return safe[:120]


def render_mermaid_flowchart(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = ["flowchart LR"]
    for machine, segs in sorted(by_machine.items()):
        lines.append(f'  subgraph {machine}')
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn, s.segment_index))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.base_urn, s.segment_index)
            prefix = f"{machine}:"
            disp = s.base_urn[len(prefix) :] if s.base_urn.startswith(prefix) else s.base_urn
            label = f"{disp}\\n{s.start_date}..{s.end_date}"
            lines.append(f'    {nid}["{label}"]')
            if prev_id is not None:
                lines.append(f"    {prev_id} --> {nid}")
            prev_id = nid
        lines.append("  end")
    return "\n".join(lines)


def render_dot(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = ["digraph fim {", "  rankdir=LR;"]
    for machine, segs in sorted(by_machine.items()):
        lines.append(f'  subgraph "cluster_{machine}" {{')
        lines.append(f'    label="{machine}";')
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn, s.segment_index))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.base_urn, s.segment_index)
            prefix = f"{machine}:"
            disp = s.base_urn[len(prefix) :] if s.base_urn.startswith(prefix) else s.base_urn
            label = f"{disp}\\n{s.start_date}..{s.end_date}"
            lines.append(f'    "{nid}" [label="{label}"];')
            if prev_id is not None:
                lines.append(f'    "{prev_id}" -> "{nid}";')
            prev_id = nid
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines)

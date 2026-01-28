from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable


@dataclass(frozen=True)
class ShaSegment:
    machine_name: str
    file_path: str
    file_name: str
    start_date: str
    end_date: str


_SAFE_NODE_RE = re.compile(r"[^a-zA-Z0-9_]+")


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


def fetch_segments_for_sha256(
    conn: sqlite3.Connection, *, sha256: str, limit: int = 20000
) -> list[ShaSegment]:
    limit = max(1, min(int(limit), 200000))
    rows = conn.execute(
        """
        SELECT machine_name, file_path, file_name, urn, scan_ts
        FROM file_record
        WHERE sha256 = ?
        ORDER BY machine_name ASC, file_path ASC, scan_ts ASC, id ASC
        LIMIT ?
        """,
        (sha256, limit),
    ).fetchall()

    grouped: dict[tuple[str, str, str], list[str]] = {}
    for r in rows:
        machine = str(r["machine_name"] or "")
        file_path = str(r["file_path"] or "")
        file_name = str(r["file_name"] or "")
        urn = str(r["urn"] or "")
        scan_ts = str(r["scan_ts"] or "")
        d = _scan_date_from_urn_or_ts(urn, scan_ts)
        grouped.setdefault((machine, file_path, file_name), []).append(d)

    segments: list[ShaSegment] = []
    for (machine, file_path, file_name), dates in grouped.items():
        unique_sorted = sorted(set(dates))
        if not unique_sorted:
            continue
        segments.append(
            ShaSegment(
                machine_name=machine,
                file_path=file_path,
                file_name=file_name,
                start_date=unique_sorted[0],
                end_date=unique_sorted[-1],
            )
        )

    segments.sort(key=lambda s: (s.machine_name, s.start_date, s.file_path, s.file_name))
    return segments


def render_ascii_chain(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = []
    for machine, segs in sorted(by_machine.items()):
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.file_path, s.file_name))
        def _disp(s: ShaSegment) -> str:
            label = s.file_name or "(no-name)"
            if s.file_path:
                return f"{label} @ {s.file_path}"
            return label
        parts = [
            f"{{{_disp(s)} {s.start_date}..{s.end_date}}}" for s in segs_sorted
        ]
        chain = " -> ".join(parts) if parts else "(no data)"
        lines.append(f"{machine} {chain}")
    return "\n".join(lines)


def _node_id(machine: str, file_path: str, file_name: str) -> str:
    raw = f"{machine}_{file_path}_{file_name}"
    safe = _SAFE_NODE_RE.sub("_", raw)
    return safe[:120]


def render_mermaid_flowchart(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = ["flowchart LR"]
    for machine, segs in sorted(by_machine.items()):
        lines.append(f'  subgraph {machine}')
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.file_path, s.file_name))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.file_path, s.file_name)
            display_path = s.file_path or "(no-path)"
            display_name = s.file_name or "(no-name)"
            label = f"{display_name}\\n{display_path}\\n{s.start_date}..{s.end_date}"
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
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.file_path, s.file_name))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.file_path, s.file_name)
            display_path = s.file_path or "(no-path)"
            display_name = s.file_name or "(no-name)"
            label = f"{display_name}\\n{display_path}\\n{s.start_date}..{s.end_date}"
            lines.append(f'    "{nid}" [label="{label}"];')
            if prev_id is not None:
                lines.append(f'    "{prev_id}" -> "{nid}";')
            prev_id = nid
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines)

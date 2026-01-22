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


def fetch_segments_for_sha256(
    conn: sqlite3.Connection, *, sha256: str, limit: int = 20000
) -> list[ShaSegment]:
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
        segments.append(
            ShaSegment(
                machine_name=machine,
                base_urn=base,
                start_date=unique_sorted[0],
                end_date=unique_sorted[-1],
            )
        )

    segments.sort(key=lambda s: (s.machine_name, s.start_date, s.base_urn))
    return segments


def render_ascii_chain(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = []
    for machine, segs in sorted(by_machine.items()):
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn))
        def _disp(base_urn: str) -> str:
            prefix = f"{machine}:"
            return base_urn[len(prefix) :] if base_urn.startswith(prefix) else base_urn
        parts = [
            f"{{{_disp(s.base_urn)} {s.start_date}..{s.end_date}}}" for s in segs_sorted
        ]
        chain = " -> ".join(parts) if parts else "(no data)"
        lines.append(f"{machine} {chain}")
    return "\n".join(lines)


def _node_id(machine: str, base_urn: str) -> str:
    raw = f"{machine}_{base_urn}"
    safe = _SAFE_NODE_RE.sub("_", raw)
    return safe[:120]


def render_mermaid_flowchart(segments: Iterable[ShaSegment]) -> str:
    by_machine: dict[str, list[ShaSegment]] = {}
    for seg in segments:
        by_machine.setdefault(seg.machine_name, []).append(seg)

    lines: list[str] = ["flowchart LR"]
    for machine, segs in sorted(by_machine.items()):
        lines.append(f'  subgraph {machine}')
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.base_urn)
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
        segs_sorted = sorted(segs, key=lambda s: (s.start_date, s.base_urn))
        prev_id: str | None = None
        for s in segs_sorted:
            nid = _node_id(machine, s.base_urn)
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

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable

from .auth import create_or_rotate_token, delete_token, list_tokens
from .db import connect, init_db
from .graph import (
    fetch_segments_for_sha256,
    render_ascii_chain,
    render_dot,
    render_mermaid_flowchart,
)


def _format_bytes(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 B"
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if size >= 10 or unit_index == 0:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def _attach_human_sizes(records: list[dict[str, object]]) -> list[dict[str, object]]:
    for record in records:
        size_value = record.get("size_bytes")
        if isinstance(size_value, int):
            record["size_human"] = _format_bytes(size_value)
    return records


def _to_str(v: object) -> str:
    if v is None:
        return ""
    return str(v)


def _print_table(
    records: list[dict[str, object]],
    columns: list[tuple[str, str]],
    max_col_width: int = 80,
) -> None:
    headers = [h for _, h in columns]
    rows: list[list[str]] = []
    for r in records:
        rows.append([_to_str(r.get(k, "")) for k, _ in columns])

    widths = []
    for i, h in enumerate(headers):
        w = len(h)
        for row in rows:
            w = max(w, len(row[i]))
        widths.append(min(w, max_col_width))

    def trunc(s: str, w: int) -> str:
        if len(s) <= w:
            return s
        return s[: w - 1] + "…"

    def fmt_row(cells: list[str]) -> str:
        return " | ".join(trunc(cells[i], widths[i]).ljust(widths[i]) for i in range(len(cells)))

    sep = "-+-".join("-" * w for w in widths)
    print(fmt_row(headers))
    print(sep)
    for row in rows:
        print(fmt_row(row))


def _cmd_query_machine(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        limit = max(1, min(int(args.limit), 5000))

        if args.sha256 is None:
            rows = conn.execute(
                """
                SELECT file_path, file_name, size_bytes, sha256, tag,
                       host_name, client_ip, scan_ts, urn
                FROM file_record
                WHERE machine_name = ?
                ORDER BY scan_ts DESC, id DESC
                LIMIT ?
                """,
                (args.machine_name, limit),
            ).fetchall()
        else:
            if len(args.sha256) != 64:
                raise SystemExit("sha256 must be 64 hex chars")
            rows = conn.execute(
                """
                SELECT file_path, file_name, size_bytes, sha256, tag,
                       host_name, client_ip, scan_ts, urn
                FROM file_record
                WHERE machine_name = ? AND sha256 = ?
                ORDER BY scan_ts DESC, id DESC
                LIMIT ?
                """,
                (args.machine_name, args.sha256, limit),
            ).fetchall()

        records = [dict(r) for r in rows]

        if args.human:
            records = _attach_human_sizes(records)

        if getattr(args, "table", False):
            for r in records:
                r["size_display"] = r.get("size_human", r.get("size_bytes", ""))

            cols = [
                ("file_name", "FILE"),
                ("file_path", "PATH"),
                ("size_display", "SIZE"),
                ("sha256", "SHA256"),
                ("tag", "TAG"),
                ("host_name", "HOST"),
                ("client_ip", "IP"),
                ("scan_ts", "SCAN_TS"),
                ("urn", "URN"),
            ]
            _print_table(records, cols)
        else:
            print(json.dumps({"machine_name": args.machine_name, "records": records}, indent=2))

        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fimserver-admin")
    p.add_argument("-H", "--human", action="store_true")
    p.add_argument("-T", "--table", action="store_true")

    sub = p.add_subparsers(dest="cmd", required=True)

    query = sub.add_parser("query")
    query_sub = query.add_subparsers(dest="query_cmd", required=True)

    machine = query_sub.add_parser("machine")
    machine.add_argument("machine_name")
    machine.add_argument("--limit", type=int, default=200)
    machine.add_argument("--sha256")
    machine.set_defaults(func=_cmd_query_machine)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())


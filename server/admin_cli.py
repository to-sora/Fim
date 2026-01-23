from __future__ import annotations

import argparse
import json
import sys

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


def _cmd_token_create(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        token = create_or_rotate_token(conn, args.machine_name)
        print(json.dumps({"machine_name": args.machine_name, "token": token}))
        return 0
    finally:
        conn.close()


def _cmd_token_list(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        print(json.dumps(list_tokens(conn), indent=2))
        return 0
    finally:
        conn.close()


def _cmd_token_delete(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        delete_token(conn, args.machine_name)
        print(json.dumps({"deleted": args.machine_name}))
        return 0
    finally:
        conn.close()


def _cmd_graph_sha256(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        segments = fetch_segments_for_sha256(conn, sha256=args.sha256, limit=args.limit)
        if args.format == "ascii":
            print(render_ascii_chain(segments))
        elif args.format == "dot":
            print(render_dot(segments))
        elif args.format == "mermaid":
            print(render_mermaid_flowchart(segments))
        else:
            print(json.dumps([s.__dict__ for s in segments], indent=2))
        return 0
    finally:
        conn.close()


def _cmd_query_file(args: argparse.Namespace) -> int:
    if len(args.sha256) != 64:
        raise SystemExit("sha256 must be 64 hex chars")
    conn = connect()
    try:
        init_db(conn)
        limit = max(1, min(int(args.limit), 1000))
        rows = conn.execute(
            """
            SELECT machine_name, file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
            FROM file_record
            WHERE sha256 = ?
            ORDER BY scan_ts DESC, id DESC
            LIMIT ?
            """,
            (args.sha256, limit),
        ).fetchall()
        records = [dict(r) for r in rows]
        if args.human:
            records = _attach_human_sizes(records)
        print(json.dumps({"sha256": args.sha256, "records": records}, indent=2))
        return 0
    finally:
        conn.close()


def _cmd_query_machine(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        init_db(conn)
        limit = max(1, min(int(args.limit), 5000))
        if args.sha256 is None:
            rows = conn.execute(
                """
                SELECT file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
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
                SELECT file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
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
        print(json.dumps({"machine_name": args.machine_name, "records": records}, indent=2))
        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fimserver-admin")
    p.add_argument(
        "-H",
        "--human",
        action="store_true",
        help="Display file sizes in human-readable form",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    token = sub.add_parser("token", help="Manage machine auth tokens")
    token_sub = token.add_subparsers(dest="token_cmd", required=True)

    create = token_sub.add_parser("create", help="Create or rotate token for a MachineName")
    create.add_argument("machine_name")
    create.set_defaults(func=_cmd_token_create)

    ls = token_sub.add_parser("list", help="List tokens")
    ls.set_defaults(func=_cmd_token_list)

    delete = token_sub.add_parser("delete", help="Delete token for a MachineName")
    delete.add_argument("machine_name")
    delete.set_defaults(func=_cmd_token_delete)

    graph = sub.add_parser("graph", help="Graph queries (CLI output)")
    graph_sub = graph.add_subparsers(dest="graph_cmd", required=True)

    sha = graph_sub.add_parser("sha256", help="Graph a target sha256 across machines/time")
    sha.add_argument("sha256")
    sha.add_argument(
        "--format",
        choices=["ascii", "dot", "mermaid", "json"],
        default="ascii",
        help="Output format",
    )
    sha.add_argument("--limit", type=int, default=20000)
    sha.set_defaults(func=_cmd_graph_sha256)

    query = sub.add_parser("query", help="Query file records (local CLI only)")
    query_sub = query.add_subparsers(dest="query_cmd", required=True)

    file_rec = query_sub.add_parser("file", help="Query records by sha256")
    file_rec.add_argument("sha256")
    file_rec.add_argument("--limit", type=int, default=100)
    file_rec.set_defaults(func=_cmd_query_file)

    machine = query_sub.add_parser("machine", help="Query records for a machine")
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

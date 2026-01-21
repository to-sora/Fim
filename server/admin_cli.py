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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fimserver-admin")
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

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())


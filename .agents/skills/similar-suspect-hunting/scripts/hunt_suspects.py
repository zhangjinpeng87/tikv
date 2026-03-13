#!/usr/bin/env python3
"""Local helper tool for stage 3 suspect hunting."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

SKILLS_ROOT = Path(__file__).resolve().parents[2]
if str(SKILLS_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILLS_ROOT))

from historical_bug_loop_common import format_source_window, read_json, utc_now_iso, write_json  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[4]


def load_stage2(path: Path) -> dict[str, Any]:
    data = read_json(path)
    if not isinstance(data, dict) or "briefs" not in data:
        raise SystemExit(f"Invalid stage2 artifact: {path}")
    return data


def select_briefs(data: dict[str, Any], bugfix_ids: list[str], start: int, limit: int) -> list[dict[str, Any]]:
    briefs = data.get("briefs", [])
    if bugfix_ids:
        wanted = set(bugfix_ids)
        selected = [brief for brief in briefs if brief.get("bugfix_id") in wanted]
        missing = sorted(wanted - {brief.get("bugfix_id") for brief in selected})
        if missing:
            raise SystemExit(f"Unknown bugfix ids: {', '.join(missing)}")
        return selected
    end = None if limit == 0 else start + limit
    return briefs[start:end]


def list_briefs(args: argparse.Namespace) -> int:
    stage2 = load_stage2(Path(args.input))
    for idx, brief in enumerate(select_briefs(stage2, args.bugfix_id, args.start, args.limit), start=1):
        print(f"{idx}\t{brief['bugfix_id']}\t{brief['title']}")
    return 0


def export_context(args: argparse.Namespace) -> int:
    stage2 = load_stage2(Path(args.input))
    selected = select_briefs(stage2, args.bugfix_id, args.start, args.limit)
    write_json(
        Path(args.output),
        {
            "repo": stage2.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage2": str(Path(args.input).resolve()),
            "briefs": selected,
        },
    )
    return 0


def search_code(args: argparse.Namespace) -> int:
    if not args.pattern:
        raise SystemExit("At least one --pattern is required")
    cmd = ["rg", "--line-number", "--with-filename", "--color", "never"]
    if not args.regex:
        cmd.append("-F")
    if args.context:
        cmd.extend(["-C", str(args.context)])
    if args.max_count:
        cmd.extend(["--max-count", str(args.max_count)])
    if args.ignore_case:
        cmd.append("-i")
    if args.hidden:
        cmd.append("--hidden")
    for glob in args.glob:
        cmd.extend(["-g", glob])
    for pattern in args.pattern:
        cmd.extend(["-e", pattern])
    cmd.append(args.path)
    completed = subprocess.run(cmd, cwd=args.repo_root, text=True)
    return completed.returncode


def show_source(args: argparse.Namespace) -> int:
    try:
        print(format_source_window(Path(args.repo_root), args.file, args.line, args.before, args.after))
    except FileNotFoundError as err:
        raise SystemExit(f"File not found: {err}") from err
    return 0


def load_suspects(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "suspects"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise SystemExit(f"Unsupported suspect file format: {path}")


def normalize_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if raw in (None, ""):
        return []
    return [raw]


def write_output(args: argparse.Namespace) -> int:
    stage2 = load_stage2(Path(args.input))
    brief_index = {brief["bugfix_id"]: brief for brief in stage2.get("briefs", [])}
    suspects = load_suspects(Path(args.suspect_file))
    results_by_bugfix: dict[str, dict[str, Any]] = {}
    errors = []
    for item in suspects:
        bugfix_id = item.get("bugfix_id")
        if bugfix_id not in brief_index:
            errors.append({"bugfix_id": bugfix_id, "error": "unknown bugfix id"})
            continue
        file_path = item.get("file")
        line = item.get("line")
        if not file_path or line is None:
            errors.append({"bugfix_id": bugfix_id, "error": "suspect missing file or line"})
            continue
        if args.repo_root and not (Path(args.repo_root) / str(file_path)).exists():
            errors.append({"bugfix_id": bugfix_id, "error": f"missing file: {file_path}"})
            continue
        bucket = results_by_bugfix.setdefault(
            bugfix_id,
            {
                "bugfix_id": bugfix_id,
                "pr_number": brief_index[bugfix_id].get("pr_number"),
                "title": brief_index[bugfix_id].get("title"),
                "brief": brief_index[bugfix_id].get("brief", {}),
                "suspects": [],
            },
        )
        suspect_id = item.get("suspect_id") or f"c{len(bucket['suspects']) + 1}"
        bucket["suspects"].append(
            {
                "suspect_id": suspect_id,
                "file": str(file_path),
                "line": int(line),
                "confidence": item.get("confidence", 0.0),
                "reason": item.get("reason") or "",
                "hypothesis": item.get("hypothesis") or "",
                "search_query": item.get("search_query") or "",
                "similarity_notes": normalize_list(item.get("similarity_notes")),
                "manual_checks": normalize_list(item.get("manual_checks")),
            }
        )
    results = list(results_by_bugfix.values())
    write_json(
        Path(args.output),
        {
            "repo": stage2.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage2": str(Path(args.input).resolve()),
            "summary": {
                "processed_bugfixes": len(results),
                "with_suspects": sum(1 for item in results if item.get("suspects")),
                "errors": len(errors),
            },
            "results": results,
            "errors": errors,
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 3 helper for similar suspect hunting.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list-briefs", help="List bugfix briefs from stage2")
    p.add_argument("--input", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=list_briefs)

    p = sub.add_parser("export-context", help="Export selected bugfix briefs for agent review")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=export_context)

    p = sub.add_parser("search-code", help="Run ripgrep for agent-chosen suspect queries")
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--pattern", action="append", default=[])
    p.add_argument("--glob", action="append", default=[])
    p.add_argument("--path", default=".")
    p.add_argument("--context", type=int, default=0)
    p.add_argument("--max-count", type=int, default=0)
    p.add_argument("--ignore-case", action="store_true")
    p.add_argument("--fixed-strings", action="store_true", help="Deprecated; literal mode is already the default.")
    p.add_argument("--regex", action="store_true", help="Treat patterns as ripgrep regex instead of safe literal strings.")
    p.add_argument("--hidden", action="store_true")
    p.set_defaults(func=search_code)

    p = sub.add_parser("show-source", help="Show numbered source around a candidate line")
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--file", required=True)
    p.add_argument("--line", required=True, type=int)
    p.add_argument("--before", type=int, default=20)
    p.add_argument("--after", type=int, default=20)
    p.set_defaults(func=show_source)

    p = sub.add_parser("write-output", help="Assemble stage3 suspects from agent-authored JSON")
    p.add_argument("--input", required=True)
    p.add_argument("--suspect-file", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.set_defaults(func=write_output)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

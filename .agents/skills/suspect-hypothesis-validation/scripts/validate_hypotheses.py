#!/usr/bin/env python3
"""Local helper tool for stage 4 suspect validation and confirmation."""

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


def load_stage3(path: Path) -> dict[str, Any]:
    data = read_json(path)
    if not isinstance(data, dict) or "results" not in data:
        raise SystemExit(f"Invalid stage3 artifact: {path}")
    return data


def iter_suspects(data: dict[str, Any], bugfix_ids: list[str], suspect_ids: list[str]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    bugfix_filter = set(bugfix_ids)
    suspect_filter = set(suspect_ids)
    selected: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for result in data.get("results", []):
        if bugfix_filter and result.get("bugfix_id") not in bugfix_filter:
            continue
        for suspect in result.get("suspects", []):
            if suspect_filter and suspect.get("suspect_id") not in suspect_filter:
                continue
            selected.append((result, suspect))
    return selected


def list_suspects(args: argparse.Namespace) -> int:
    stage3 = load_stage3(Path(args.input))
    pairs = iter_suspects(stage3, args.bugfix_id, args.suspect_id)
    if args.limit:
        pairs = pairs[: args.limit]
    for idx, (result, suspect) in enumerate(pairs, start=1):
        print(f"{idx}\t{result['bugfix_id']}\t{suspect['suspect_id']}\t{suspect['file']}:{suspect['line']}\t{result['title']}")
    return 0


def collect_context(args: argparse.Namespace) -> int:
    stage3 = load_stage3(Path(args.input))
    repo_root = Path(args.repo_root)
    contexts = []
    errors = []
    for result, suspect in iter_suspects(stage3, args.bugfix_id, args.suspect_id):
        try:
            source_context = format_source_window(repo_root, suspect["file"], int(suspect["line"]), args.before, args.after)
        except FileNotFoundError:
            errors.append({
                "bugfix_id": result.get("bugfix_id"),
                "suspect_id": suspect.get("suspect_id"),
                "error": f"missing file: {suspect.get('file')}",
            })
            continue
        contexts.append(
            {
                "bugfix_id": result.get("bugfix_id"),
                "pr_number": result.get("pr_number"),
                "title": result.get("title"),
                "brief": result.get("brief", {}),
                "suspect": suspect,
                "source_context": source_context,
            }
        )
    write_json(
        Path(args.output),
        {
            "repo": stage3.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage3": str(Path(args.input).resolve()),
            "context_window": {"before": args.before, "after": args.after},
            "suspects": contexts,
            "errors": errors,
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


def load_verdicts(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("verdicts", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise SystemExit(f"Unsupported verdict file format: {path}")


def normalize_reproduction_plan(raw: dict[str, Any] | None) -> dict[str, Any]:
    raw = raw or {}
    return {
        "test_type": raw.get("test_type") or "",
        "target_file": raw.get("target_file") or "",
        "target_test": raw.get("target_test") or "",
        "setup": raw.get("setup") or "",
        "test_code": raw.get("test_code") or "",
        "run_command": raw.get("run_command") or "",
        "expected_failure_signal": raw.get("expected_failure_signal") or "",
    }


def build_suspect_index(stage3: dict[str, Any]) -> dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]]:
    index: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
    for result in stage3.get("results", []):
        for suspect in result.get("suspects", []):
            index[(result.get("bugfix_id"), suspect.get("suspect_id"))] = (result, suspect)
    return index


def write_output(args: argparse.Namespace) -> int:
    stage3 = load_stage3(Path(args.input))
    verdicts = load_verdicts(Path(args.verdict_file))
    suspect_index = build_suspect_index(stage3)
    confirmed = []
    rejected = []
    errors = []
    touched_bugfixes: set[str] = set()
    for item in verdicts:
        bugfix_id = item.get("bugfix_id")
        suspect_id = item.get("suspect_id")
        key = (bugfix_id, suspect_id)
        if key not in suspect_index:
            errors.append({"bugfix_id": bugfix_id, "suspect_id": suspect_id, "error": "unknown suspect reference"})
            continue
        verdict = (item.get("verdict") or "").lower()
        if verdict not in {"confirmed", "rejected", "uncertain"}:
            errors.append({"bugfix_id": bugfix_id, "suspect_id": suspect_id, "error": f"unsupported verdict: {verdict or '<missing>'}"})
            continue
        result, suspect = suspect_index[key]
        touched_bugfixes.add(bugfix_id)
        entry = {
            "bugfix_id": bugfix_id,
            "pr_number": result.get("pr_number"),
            "title": result.get("title"),
            "suspect_id": suspect_id,
            "location": {"file": suspect.get("file"), "line": int(suspect.get("line"))},
            "confidence": item.get("confidence", suspect.get("confidence", 0.0)),
            "hypothesis": item.get("hypothesis") or suspect.get("hypothesis") or "",
            "reason": item.get("reason") or "",
            "evidence": item.get("evidence") or "",
            "reproduction_plan": normalize_reproduction_plan(item.get("reproduction_plan")),
            "fix_hint": item.get("fix_hint") or "",
        }
        if verdict == "confirmed":
            confirmed.append(entry)
        else:
            entry["verdict"] = verdict
            entry["filter_reason"] = item.get("filter_reason") or ""
            rejected.append(entry)
    write_json(
        Path(args.output),
        {
            "repo": stage3.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage3": str(Path(args.input).resolve()),
            "summary": {
                "processed_bugfixes": len(touched_bugfixes),
                "processed_suspects": len(confirmed) + len(rejected),
                "confirmed": len(confirmed),
                "rejected_or_uncertain": len(rejected),
                "errors": len(errors),
            },
            "confirmed_bugs": confirmed,
            "rejected_or_uncertain": rejected,
            "errors": errors,
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 4 helper for suspect validation and confirmation.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list-suspects", help="List suspects from stage3")
    p.add_argument("--input", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--suspect-id", action="append", default=[])
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=list_suspects)

    p = sub.add_parser("collect-context", help="Collect source context for selected suspects")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--suspect-id", action="append", default=[])
    p.add_argument("--before", type=int, default=25)
    p.add_argument("--after", type=int, default=25)
    p.set_defaults(func=collect_context)

    p = sub.add_parser("search-code", help="Run ripgrep when hunting for test hooks or nearby cases")
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

    p = sub.add_parser("show-source", help="Show numbered source around a suspect or test line")
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--file", required=True)
    p.add_argument("--line", required=True, type=int)
    p.add_argument("--before", type=int, default=25)
    p.add_argument("--after", type=int, default=25)
    p.set_defaults(func=show_source)

    p = sub.add_parser("write-output", help="Assemble stage4 confirmations from agent-authored JSON")
    p.add_argument("--input", required=True)
    p.add_argument("--verdict-file", required=True)
    p.add_argument("--output", required=True)
    p.set_defaults(func=write_output)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

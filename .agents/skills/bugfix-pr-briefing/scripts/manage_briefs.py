#!/usr/bin/env python3
"""Local helper tool for stage 2 bugfix PR briefing."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

SKILLS_ROOT = Path(__file__).resolve().parents[2]
if str(SKILLS_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILLS_ROOT))

from historical_bug_loop_common import read_json, utc_now_iso, write_json  # noqa: E402


def load_stage1(path: Path) -> dict[str, Any]:
    data = read_json(path)
    if not isinstance(data, dict) or "bugfixes" not in data:
        raise SystemExit(f"Invalid stage1 artifact: {path}")
    return data


def select_bugfixes(data: dict[str, Any], bugfix_ids: list[str], start: int, limit: int) -> list[dict[str, Any]]:
    bugfixes = data.get("bugfixes", [])
    if bugfix_ids:
        wanted = set(bugfix_ids)
        selected = [bugfix for bugfix in bugfixes if bugfix.get("id") in wanted]
        missing = sorted(wanted - {bugfix.get("id") for bugfix in selected})
        if missing:
            raise SystemExit(f"Unknown bugfix ids: {', '.join(missing)}")
        return selected
    end = None if limit == 0 else start + limit
    return bugfixes[start:end]


def list_seeds(args: argparse.Namespace) -> int:
    stage1 = load_stage1(Path(args.input))
    for idx, bugfix in enumerate(select_bugfixes(stage1, args.bugfix_id, args.start, args.limit), start=1):
        print(f"{idx}\t{bugfix['id']}\tfiles={len(bugfix.get('changed_files', []))}\t{bugfix['title']}")
    return 0


def export_context(args: argparse.Namespace) -> int:
    stage1 = load_stage1(Path(args.input))
    selected = select_bugfixes(stage1, args.bugfix_id, args.start, args.limit)
    exported = []
    for bugfix in selected:
        changed_files = []
        for item in bugfix.get("changed_files", [])[: args.max_files]:
            changed_files.append(
                {
                    "path": item.get("path"),
                    "status": item.get("status"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "changes": item.get("changes"),
                    "patch": (item.get("patch") or "")[: args.patch_chars],
                }
            )
        exported.append(
            {
                "bugfix_id": bugfix.get("id"),
                "pr_number": bugfix.get("number"),
                "title": bugfix.get("title"),
                "url": bugfix.get("url"),
                "merged_at": bugfix.get("merged_at"),
                "labels": bugfix.get("labels", []),
                "body": (bugfix.get("body") or "")[: args.body_chars],
                "reviews": bugfix.get("reviews", [])[: args.max_reviews],
                "stats": bugfix.get("stats", {}),
                "classification_signals": bugfix.get("classification_signals", {}),
                "diff_excerpt": (bugfix.get("diff_excerpt") or "")[: args.diff_chars],
                "changed_files": changed_files,
                "exclude_paths": sorted((bugfix.get("modified_line_ranges") or {}).keys()),
                "modified_line_ranges": bugfix.get("modified_line_ranges", {}),
            }
        )
    write_json(
        Path(args.output),
        {
            "repo": stage1.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage1": str(Path(args.input).resolve()),
            "bugfixes": exported,
        },
    )
    return 0


def load_briefs(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("briefs", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise SystemExit(f"Unsupported brief file format: {path}")


def normalize_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if raw in (None, ""):
        return []
    return [raw]


def write_output(args: argparse.Namespace) -> int:
    stage1 = load_stage1(Path(args.input))
    bugfix_index = {bugfix["id"]: bugfix for bugfix in stage1.get("bugfixes", [])}
    briefs = load_briefs(Path(args.brief_file))
    out_briefs = []
    errors = []
    for item in briefs:
        bugfix_id = item.get("bugfix_id")
        if bugfix_id not in bugfix_index:
            errors.append({"bugfix_id": bugfix_id, "error": "unknown bugfix id"})
            continue
        bugfix = bugfix_index[bugfix_id]
        out_briefs.append(
            {
                "bugfix_id": bugfix_id,
                "pr_number": bugfix.get("number"),
                "title": bugfix.get("title"),
                "url": bugfix.get("url"),
                "brief": {
                    "seed_summary": item.get("seed_summary") or "",
                    "root_cause": item.get("root_cause") or "",
                    "trigger_conditions": normalize_list(item.get("trigger_conditions")),
                    "violated_invariants": normalize_list(item.get("violated_invariants")),
                    "fix_strategy": item.get("fix_strategy") or "",
                    "search_queries": normalize_list(item.get("search_queries")),
                    "suspicious_signals": normalize_list(item.get("suspicious_signals")),
                    "exclude_paths": normalize_list(item.get("exclude_paths"))
                    or sorted((bugfix.get("modified_line_ranges") or {}).keys()),
                    "confidence_notes": item.get("confidence_notes") or "",
                },
            }
        )
    write_json(
        Path(args.output),
        {
            "repo": stage1.get("repo"),
            "generated_at": utc_now_iso(),
            "source_stage1": str(Path(args.input).resolve()),
            "summary": {"processed_bugfixes": len(out_briefs), "errors": len(errors)},
            "briefs": out_briefs,
            "errors": errors,
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 2 helper for bugfix PR briefing.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list-seeds", help="List bugfix seeds from a stage1 artifact")
    p.add_argument("--input", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=list_seeds)

    p = sub.add_parser("export-context", help="Export compact PR context for agent briefing")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--max-files", type=int, default=20)
    p.add_argument("--max-reviews", type=int, default=10)
    p.add_argument("--body-chars", type=int, default=6000)
    p.add_argument("--patch-chars", type=int, default=1200)
    p.add_argument("--diff-chars", type=int, default=12000)
    p.set_defaults(func=export_context)

    p = sub.add_parser("write-output", help="Assemble stage2 briefs from agent-authored JSON")
    p.add_argument("--input", required=True)
    p.add_argument("--brief-file", required=True)
    p.add_argument("--output", required=True)
    p.set_defaults(func=write_output)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

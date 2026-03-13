#!/usr/bin/env python3
"""Build and slice historical bugfix seed sets."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import Any

SKILLS_ROOT = Path(__file__).resolve().parents[2]
if str(SKILLS_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILLS_ROOT))

from historical_bug_loop_common import (  # noqa: E402
    GitHubClient,
    classify_pr,
    compact_files,
    compact_reviews,
    extract_modified_line_ranges,
    make_diff_excerpt,
    parse_iso8601,
    read_json,
    utc_now_iso,
    write_json,
)


def mine_seeds(args: argparse.Namespace) -> int:
    token = args.github_token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("Missing GitHub token. Set --github-token or GITHUB_TOKEN/GH_TOKEN.")

    merged_since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.months * 30)).date().isoformat()
    client = GitHubClient(token=token, base_url=args.github_api_base)
    pr_numbers = client.search_merged_pr_numbers(args.owner, args.repo, merged_since, args.max_prs)

    bugfixes: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for idx, number in enumerate(pr_numbers, start=1):
        if args.verbose:
            print(f"[seed] [{idx}/{len(pr_numbers)}] PR #{number}")

        pr = client.get_pull(args.owner, args.repo, number)
        merged_at = parse_iso8601(pr.get("merged_at"))
        if not merged_at:
            skipped.append({"number": number, "reason": "not_merged"})
            continue
        if merged_at.date().isoformat() < merged_since:
            skipped.append({"number": number, "reason": "outside_window"})
            continue

        files = client.list_pull_files(args.owner, args.repo, number)
        reviews = client.list_pull_reviews(args.owner, args.repo, number)
        selected, reason, signals = classify_pr(pr, files)
        if not selected:
            skipped.append({"number": number, "reason": reason, "signals": signals})
            continue

        compacted_files = compact_files(files)
        modified_ranges = {
            item["path"]: extract_modified_line_ranges(item.get("patch") or "")
            for item in compacted_files
            if item.get("path")
        }
        bugfixes.append(
            {
                "id": f"{args.owner}/{args.repo}#{number}",
                "number": number,
                "title": pr.get("title"),
                "url": pr.get("html_url"),
                "merged_at": pr.get("merged_at"),
                "author": (pr.get("user") or {}).get("login"),
                "labels": [label.get("name", "") for label in pr.get("labels", [])],
                "body": pr.get("body") or "",
                "reviews": compact_reviews(reviews),
                "stats": {
                    "additions": pr.get("additions"),
                    "deletions": pr.get("deletions"),
                    "changed_files": pr.get("changed_files"),
                    "commits": pr.get("commits"),
                },
                "changed_files": compacted_files,
                "diff_excerpt": make_diff_excerpt(files),
                "modified_line_ranges": modified_ranges,
                "classification_signals": signals,
            }
        )
        if len(bugfixes) >= args.max_selected:
            break

    output = {
        "repo": f"{args.owner}/{args.repo}",
        "generated_at": utc_now_iso(),
        "window": {"months": args.months, "merged_since": merged_since},
        "summary": {
            "scanned_prs": len(pr_numbers),
            "selected_bugfixes": len(bugfixes),
            "skipped": len(skipped),
        },
        "bugfixes": bugfixes,
        "skipped": skipped,
    }
    write_json(Path(args.output), output)
    return 0


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
    data = read_json(Path(args.input))
    for idx, bugfix in enumerate(select_bugfixes(data, args.bugfix_id, args.start, args.limit), start=1):
        print(f"{idx}\t{bugfix['id']}\tfiles={len(bugfix.get('changed_files', []))}\t{bugfix['title']}")
    return 0


def slice_seeds(args: argparse.Namespace) -> int:
    data = read_json(Path(args.input))
    selected = select_bugfixes(data, args.bugfix_id, args.start, args.limit)
    data["bugfixes"] = selected
    data["summary"]["selected_bugfixes"] = len(selected)
    write_json(Path(args.output), data)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and slice historical bugfix seed sets.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("mine", help="Fetch recent merged bugfix PRs and write stage1 seeds")
    p.add_argument("--owner", required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--months", type=int, default=3)
    p.add_argument("--output", required=True)
    p.add_argument("--github-token")
    p.add_argument("--github-api-base", default="https://api.github.com")
    p.add_argument("--max-prs", type=int, default=200)
    p.add_argument("--max-selected", type=int, default=50)
    p.add_argument("--verbose", action="store_true")
    p.set_defaults(func=mine_seeds)

    p = sub.add_parser("list", help="List bugfix seeds from a stage1 artifact")
    p.add_argument("--input", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=list_seeds)

    p = sub.add_parser("slice", help="Write a subset stage1 artifact for focused batches")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--bugfix-id", action="append", default=[])
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(func=slice_seeds)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

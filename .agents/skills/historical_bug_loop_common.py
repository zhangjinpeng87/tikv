from __future__ import annotations

import datetime as dt
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


BUG_KEYWORDS = (
    "bug",
    "fix",
    "panic",
    "crash",
    "avoid",
    "prevent",
    "reject",
    "starvation",
    "race",
    "deadlock",
    "corrupt",
    "incorrect",
    "wrong",
    "hang",
    "overflow",
    "underflow",
    "leak",
    "fault",
    "null",
    "none",
    "invalid",
    "regression",
)

TRIVIAL_KEYWORDS = (
    "typo",
    "spelling",
    "readme",
    "docs",
    "doc",
    "comment",
    "format",
    "clippy",
    "lint",
    "chore",
    "refactor",
    "cleanup",
    "config",
    "ci",
    "workflow",
)

DOC_CONFIG_PREFIXES = (
    ".github/",
    "doc/",
    "docs/",
    "images/",
    "etc/",
    "ci-build/",
)

DOC_CONFIG_EXTS = {
    ".md",
    ".rst",
    ".txt",
    ".png",
    ".jpg",
    ".jpeg",
    ".svg",
    ".yml",
    ".yaml",
    ".toml",
    ".json",
    ".ini",
    ".lock",
}

CODE_EXTS = {
    ".rs",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".go",
    ".java",
    ".py",
}

NON_RUNTIME_CODE_PREFIXES = (
    "tests/",
    "fuzz/",
    "scripts/",
    "metrics/",
    "benches/",
    "examples/",
)

FEATURE_KEYWORDS = (
    "support ",
    "support:",
    "add support",
    "enable ",
    "introduce ",
    "dashboard",
    "grafana",
    "metric",
    "compaction",
    "optimize",
    "improve",
    "performance",
)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def parse_iso8601(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def clamp_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "\n... [truncated]"


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)
        fh.write("\n")


def path_is_docs_or_config(path: str) -> bool:
    lowered = path.lower()
    if lowered.startswith("tests/") or "/tests/" in lowered:
        return True
    if lowered.startswith("fuzz/"):
        return True
    if any(lowered.startswith(prefix) for prefix in DOC_CONFIG_PREFIXES):
        return True
    return Path(lowered).suffix in DOC_CONFIG_EXTS


def path_is_code(path: str) -> bool:
    return Path(path.lower()).suffix in CODE_EXTS


def path_is_runtime_code(path: str) -> bool:
    lowered = path.lower()
    if not path_is_code(lowered):
        return False
    if path_is_docs_or_config(lowered):
        return False
    if any(lowered.startswith(prefix) for prefix in NON_RUNTIME_CODE_PREFIXES):
        return False
    return True


def text_has_any_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def extract_modified_line_ranges(patch: str) -> list[tuple[int, int]]:
    if not patch:
        return []
    ranges: list[tuple[int, int]] = []
    hunk_pattern = re.compile(r"@@\s+-\d+(?:,\d+)?\s+\+(\d+)(?:,(\d+))?\s+@@")
    for line in patch.splitlines():
        match = hunk_pattern.search(line)
        if not match:
            continue
        start = int(match.group(1))
        length = int(match.group(2) or "1")
        end = start + max(length - 1, 0)
        ranges.append((start, end))
    return ranges


def format_source_window(repo_root: Path, rel_path: str, line: int, before: int, after: int) -> str:
    path = repo_root / rel_path
    if not path.exists():
        raise FileNotFoundError(rel_path)
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return "<empty file>"
    line = max(1, min(line, len(lines)))
    start = max(1, line - before)
    end = min(len(lines), line + after)
    width = len(str(end))
    rendered: list[str] = []
    for lineno in range(start, end + 1):
        marker = ">" if lineno == line else " "
        rendered.append(f"{marker} {lineno:>{width}} | {lines[lineno - 1]}")
    return "\n".join(rendered)


class GitHubClient:
    def __init__(self, token: str, base_url: str = "https://api.github.com") -> None:
        self.token = token
        self.base_url = base_url.rstrip("/")

    def _request(self, url: str, accept: str = "application/vnd.github+json") -> Any:
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": accept,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "historical-bug-loop",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if "application/json" in resp.headers.get("Content-Type", ""):
                    return json.loads(raw)
                return raw
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API request failed ({err.code}) {url}: {body}") from err

    def search_merged_pr_numbers(self, owner: str, repo: str, merged_since: str, max_prs: int) -> list[int]:
        numbers: list[int] = []
        page = 1
        while len(numbers) < max_prs:
            q = f"repo:{owner}/{repo} is:pr is:merged merged:>={merged_since}"
            params = urllib.parse.urlencode(
                {
                    "q": q,
                    "sort": "updated",
                    "order": "desc",
                    "per_page": "100",
                    "page": str(page),
                }
            )
            payload = self._request(f"{self.base_url}/search/issues?{params}")
            items = payload.get("items", [])
            if not items:
                break
            for item in items:
                numbers.append(int(item["number"]))
                if len(numbers) >= max_prs:
                    break
            page += 1
        return numbers

    def get_pull(self, owner: str, repo: str, number: int) -> dict[str, Any]:
        return self._request(f"{self.base_url}/repos/{owner}/{repo}/pulls/{number}")

    def list_pull_files(self, owner: str, repo: str, number: int) -> list[dict[str, Any]]:
        files: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(
                f"{self.base_url}/repos/{owner}/{repo}/pulls/{number}/files?per_page=100&page={page}"
            )
            if not payload:
                break
            files.extend(payload)
            page += 1
        return files

    def list_pull_reviews(self, owner: str, repo: str, number: int) -> list[dict[str, Any]]:
        reviews: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(
                f"{self.base_url}/repos/{owner}/{repo}/pulls/{number}/reviews?per_page=100&page={page}"
            )
            if not payload:
                break
            reviews.extend(payload)
            page += 1
        return reviews


def classify_pr(pr: dict[str, Any], files: list[dict[str, Any]]) -> tuple[bool, str, dict[str, Any]]:
    title = pr.get("title", "")
    body = pr.get("body") or ""
    labels = [label.get("name", "") for label in pr.get("labels", [])]
    title_text = title.lower()
    title_body_text = f"{title}\n{body}".lower()
    label_text = " ".join(labels).lower()

    paths = [item.get("filename", "") for item in files]
    has_code_changes = any(path_is_code(path) for path in paths)
    has_runtime_code_changes = any(path_is_runtime_code(path) for path in paths)
    all_docs_or_config = bool(paths) and all(path_is_docs_or_config(path) for path in paths)

    title_bug_signal = text_has_any_keyword(title_text, BUG_KEYWORDS)
    body_bug_signal = text_has_any_keyword(title_body_text, BUG_KEYWORDS)
    label_bug_signal = "bug" in label_text or "fix" in label_text
    feature_signal = text_has_any_keyword(title_text, FEATURE_KEYWORDS)
    trivial_signal = text_has_any_keyword(title_text, TRIVIAL_KEYWORDS)

    signals = {
        "title_bug_signal": title_bug_signal,
        "body_bug_signal": body_bug_signal,
        "label_bug_signal": label_bug_signal,
        "feature_signal": feature_signal,
        "trivial_signal": trivial_signal,
        "has_code_changes": has_code_changes,
        "has_runtime_code_changes": has_runtime_code_changes,
        "all_docs_or_config": all_docs_or_config,
    }

    if all_docs_or_config:
        return False, "trivial_docs_config_or_tests_only", signals
    if trivial_signal and not has_code_changes:
        return False, "trivial_non_code_change", signals
    if not has_runtime_code_changes:
        return False, "no_runtime_code_change", signals
    if feature_signal and not title_bug_signal:
        return False, "feature_like_change", signals
    if not title_bug_signal:
        return False, "missing_bugfix_signal", signals
    return True, "selected", signals


def compact_reviews(reviews: list[dict[str, Any]], max_items: int = 20) -> list[dict[str, Any]]:
    compacted = []
    for review in reviews[:max_items]:
        compacted.append(
            {
                "user": (review.get("user") or {}).get("login"),
                "state": review.get("state"),
                "submitted_at": review.get("submitted_at"),
                "body": clamp_text(review.get("body") or "", 600),
                "commit_id": review.get("commit_id"),
            }
        )
    return compacted


def compact_files(files: list[dict[str, Any]], max_patch_chars: int = 5000) -> list[dict[str, Any]]:
    compacted = []
    for item in files:
        compacted.append(
            {
                "path": item.get("filename"),
                "status": item.get("status"),
                "additions": item.get("additions"),
                "deletions": item.get("deletions"),
                "changes": item.get("changes"),
                "patch": clamp_text(item.get("patch") or "", max_patch_chars),
            }
        )
    return compacted


def make_diff_excerpt(files: list[dict[str, Any]], max_chars: int = 35000) -> str:
    parts: list[str] = []
    for item in files:
        filename = item.get("filename") or ""
        patch = item.get("patch") or ""
        if not filename or not patch:
            continue
        parts.append(f"diff --git a/{filename} b/{filename}\n{patch}\n")
    return clamp_text("\n".join(parts), max_chars)

# Historical Bugfix Learning Loop

This repo now uses a skill-first loop instead of a monolithic pipeline script.

The loop is designed around Codex agent reasoning with local helper tools at each stage. The agent is responsible for interpretation, hypothesis generation, and validation. The tools are responsible only for local tasks such as fetching PR metadata, exporting compact context, searching the repo, showing source, and assembling JSON artifacts.

## Stage Map

- Coordinator: `.agents/skills/historical-bug-loop/SKILL.md`
- Stage 1: `.agents/skills/historical-bugfix-seeding/SKILL.md`
- Stage 2: `.agents/skills/bugfix-pr-briefing/SKILL.md`
- Stage 3: `.agents/skills/similar-suspect-hunting/SKILL.md`
- Stage 4: `.agents/skills/suspect-hypothesis-validation/SKILL.md`

## Why This Split

The original workflow had two distinct reasoning steps that should not be merged:

1. understand the historical bugfix PR itself
2. search current HEAD for similar but unfixed code

Those are now different stages, which makes the loop easier to audit and less likely to drift into false positives.

## Artifact Chain

- Stage 1: `artifacts/quality-enhance-loop/stage1_bugfix_seeds.json`
- Stage 2: `artifacts/quality-enhance-loop/stage2_bugfix_briefs.json`
- Stage 3: `artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json`
- Stage 4: `artifacts/quality-enhance-loop/stage4_confirmed_bugs.json`

## Requirements

- Python 3.10+
- `rg` on PATH
- GitHub token for Stage 1 (`GITHUB_TOKEN` or `GH_TOKEN`)

No stage-local tool in Stages 2-4 calls an LLM.

`search-code` in Stages 3 and 4 treats patterns as literal strings by default. Use `--regex` only for intentional ripgrep regex queries.

## Recommended Operating Pattern

1. Refresh the seed set with Stage 1.
2. Slice a small batch.
3. For each bugfix PR in that batch, run Stage 2 and write one brief.
4. For that brief, run Stage 3 and keep at most 3 suspects.
5. For each suspect, run Stage 4 and either confirm or reject it.
6. Only after manual review should anything leave the repo as a GitHub issue or patch.

## Quality Gates

- Do not treat a seed PR as proof of a new bug pattern unless the fix rationale is clear.
- Do not keep a suspect if it is only naming similarity, an import, or already-fixed code.
- Do not confirm a bug without a current-HEAD failure story and a concrete reproduction plan.

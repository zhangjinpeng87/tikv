---
name: historical-bugfix-seeding
description: Build the stage1 historical bugfix seed set from recent merged PRs, skip trivial fixes, and slice batches for later agent-driven analysis.
---

# Historical Bugfix Seeding

Use this skill for Stage 1.

## What The Agent Does

- choose the repository and time window
- decide how many seed PRs to keep
- decide whether a slice should be created for a focused batch

## What The Local Tool Does

- fetch merged PRs through the GitHub API
- keep likely bug-fix PRs
- skip trivial docs/config/tests-only changes
- write the normalized stage1 seed artifact
- slice stage1 artifacts into smaller batches

## Local Tool

```bash
python3 .agents/skills/historical-bugfix-seeding/scripts/build_seed_set.py --help
```

Examples:

```bash
python3 .agents/skills/historical-bugfix-seeding/scripts/build_seed_set.py mine \
  --owner tikv \
  --repo tikv \
  --months 3 \
  --output artifacts/quality-enhance-loop/stage1_bugfix_seeds.json

python3 .agents/skills/historical-bugfix-seeding/scripts/build_seed_set.py list \
  --input artifacts/quality-enhance-loop/stage1_bugfix_seeds.json \
  --start 0 \
  --limit 10

python3 .agents/skills/historical-bugfix-seeding/scripts/build_seed_set.py slice \
  --input artifacts/quality-enhance-loop/stage1_bugfix_seeds.json \
  --output artifacts/quality-enhance-loop/stage1_bugfix_seeds_batch.json \
  --start 0 \
  --limit 5
```

## Requirements

- `GITHUB_TOKEN` or `GH_TOKEN`, or pass `--github-token`

## Output Artifact

- `artifacts/quality-enhance-loop/stage1_bugfix_seeds.json`

## Guardrails

- Stage 1 output is only a seed set, not proof of a real bugfix.
- Do not overwrite the main seed artifact when creating focused batches.

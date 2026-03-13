---
name: similar-suspect-hunting
description: Use a stage2 bugfix brief to search the current codebase for similar unfixed suspects, then assemble the stage3 suspect-hypothesis artifact.
---

# Similar Suspect Hunting

Use this skill for Stage 3.

## What The Agent Does

- read one bugfix brief
- choose search queries from the brief
- inspect current code
- decide which locations are plausible unfixed suspects
- write a small suspect set with explicit hypotheses

## What The Local Tool Does

- export selected bugfix briefs
- run `rg` searches over the repo
- show source around candidate lines
- assemble the final stage3 suspect artifact from agent-authored JSON

## Local Tool

```bash
python3 .agents/skills/similar-suspect-hunting/scripts/hunt_suspects.py --help
```

`search-code` treats patterns as literal strings by default. Pass `--regex` only when you intentionally want ripgrep regex semantics.

Examples:

```bash
python3 .agents/skills/similar-suspect-hunting/scripts/hunt_suspects.py export-context \
  --input artifacts/quality-enhance-loop/stage2_bugfix_briefs.json \
  --output artifacts/quality-enhance-loop/stage3_hunt_input.json \
  --bugfix-id tikv/tikv#19313

python3 .agents/skills/similar-suspect-hunting/scripts/hunt_suspects.py search-code \
  --repo-root . \
  --pattern 'after_ts >= latest_write_commit_ts' \
  --glob '*.rs' \
  --context 2
```

After the agent writes `stage3_agent_suspects.json`, assemble:

```bash
python3 .agents/skills/similar-suspect-hunting/scripts/hunt_suspects.py write-output \
  --input artifacts/quality-enhance-loop/stage2_bugfix_briefs.json \
  --suspect-file artifacts/quality-enhance-loop/stage3_agent_suspects.json \
  --output artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json \
  --repo-root .
```

## Agent-Authored Suspect Schema

```json
[
  {
    "bugfix_id": "tikv/tikv#12345",
    "suspect_id": "c1",
    "file": "src/foo.rs",
    "line": 42,
    "confidence": 0.72,
    "reason": "...",
    "hypothesis": "...",
    "search_query": "...",
    "similarity_notes": ["..."],
    "manual_checks": ["..."]
  }
]
```

## Output Artifact

- `artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json`

## Guardrails

- Up to 3 suspects per seed PR unless the user asks otherwise.
- Exclude seed-fix files by default.
- A suspect is only a hypothesis, not a confirmed bug.

---
name: bugfix-pr-briefing
description: Extract the key lesson from one historical bug-fix PR at a time and assemble the stage2 bugfix-brief artifact for later suspect hunting.
---

# Bugfix PR Briefing

Use this skill for Stage 2.

## Why This Stage Exists

This stage is separate from suspect hunting. Its only job is to learn the bug pattern from the historical PR.

The agent should extract:

- what went wrong
- why it went wrong
- what invariant was violated
- how the fix prevents it
- what search queries or code signals should be used later

## What The Local Tool Does

- list seed PRs from stage1
- export compact PR context for agent analysis
- assemble the final stage2 brief artifact from agent-authored JSON

## Local Tool

```bash
python3 .agents/skills/bugfix-pr-briefing/scripts/manage_briefs.py --help
```

Examples:

```bash
python3 .agents/skills/bugfix-pr-briefing/scripts/manage_briefs.py export-context \
  --input artifacts/quality-enhance-loop/stage1_bugfix_seeds.json \
  --output artifacts/quality-enhance-loop/stage2_brief_input.json \
  --bugfix-id tikv/tikv#19313
```

After the agent writes `stage2_agent_briefs.json`, assemble:

```bash
python3 .agents/skills/bugfix-pr-briefing/scripts/manage_briefs.py write-output \
  --input artifacts/quality-enhance-loop/stage1_bugfix_seeds.json \
  --brief-file artifacts/quality-enhance-loop/stage2_agent_briefs.json \
  --output artifacts/quality-enhance-loop/stage2_bugfix_briefs.json
```

## Agent-Authored Brief Schema

```json
[
  {
    "bugfix_id": "tikv/tikv#12345",
    "seed_summary": "...",
    "root_cause": "...",
    "trigger_conditions": ["..."],
    "violated_invariants": ["..."],
    "fix_strategy": "...",
    "search_queries": ["..."],
    "suspicious_signals": ["..."],
    "exclude_paths": ["src/already/fixed.rs"],
    "confidence_notes": "..."
  }
]
```

## Output Artifact

- `artifacts/quality-enhance-loop/stage2_bugfix_briefs.json`

## Guardrails

- Work one bugfix PR at a time.
- Do not hunt suspects yet.
- If the PR context is ambiguous, record that in `confidence_notes` instead of overcommitting.

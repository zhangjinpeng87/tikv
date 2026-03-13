---
name: suspect-hypothesis-validation
description: Validate stage3 suspect hypotheses on current HEAD, design repro tests, and assemble the stage4 confirmed-bug artifact.
---

# Suspect Hypothesis Validation

Use this skill for Stage 4.

## What The Agent Does

- inspect the suspect location on current HEAD
- decide confirmed vs rejected vs uncertain
- construct a reproduction plan or test case for the hypothesis
- record the evidence that justifies the verdict

## What The Local Tool Does

- list suspects from stage3
- collect source context for selected suspects
- search the repo for nearby tests or hooks
- show source around suspect or test lines
- assemble the final stage4 artifact from agent-authored JSON

## Local Tool

```bash
python3 .agents/skills/suspect-hypothesis-validation/scripts/validate_hypotheses.py --help
```

`search-code` treats patterns as literal strings by default. Pass `--regex` only when you intentionally want ripgrep regex semantics.

Examples:

```bash
python3 .agents/skills/suspect-hypothesis-validation/scripts/validate_hypotheses.py collect-context \
  --input artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json \
  --output artifacts/quality-enhance-loop/stage4_validation_input.json \
  --repo-root . \
  --bugfix-id tikv/tikv#19313

python3 .agents/skills/suspect-hypothesis-validation/scripts/validate_hypotheses.py search-code \
  --repo-root . \
  --pattern 'failpoint' \
  --glob '*.rs' \
  --context 2
```

After the agent writes `stage4_agent_verdicts.json`, assemble:

```bash
python3 .agents/skills/suspect-hypothesis-validation/scripts/validate_hypotheses.py write-output \
  --input artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json \
  --verdict-file artifacts/quality-enhance-loop/stage4_agent_verdicts.json \
  --output artifacts/quality-enhance-loop/stage4_confirmed_bugs.json
```

## Agent-Authored Verdict Schema

```json
[
  {
    "bugfix_id": "tikv/tikv#12345",
    "suspect_id": "c1",
    "verdict": "confirmed",
    "confidence": 0.84,
    "hypothesis": "...",
    "reason": "...",
    "evidence": "...",
    "fix_hint": "...",
    "reproduction_plan": {
      "test_type": "failpoint",
      "target_file": "tests/failpoints/cases/repro_x.rs",
      "target_test": "repro_x",
      "setup": "...",
      "test_code": "...",
      "run_command": "cargo test repro_x --test failpoints",
      "expected_failure_signal": "assertion fails before fix"
    }
  }
]
```

## Output Artifact

- `artifacts/quality-enhance-loop/stage4_confirmed_bugs.json`

## Guardrails

- Require current-HEAD evidence, not only historical similarity.
- Reject fix-like code, imports, helpers, and test-only locations.
- A reproduction plan should explain the expected failure signal before the fix.
- Do not file GitHub issues blindly from Stage 4 output.

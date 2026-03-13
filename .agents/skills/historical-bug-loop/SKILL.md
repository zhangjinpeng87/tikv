---
name: historical-bug-loop
description: Coordinator skill for the full historical bugfix learning loop. Use when the task spans multiple stages: mine seed PRs, distill bugfix lessons, hunt similar suspects, construct repro tests, and confirm bugs.
---

# Historical Bug Loop

Use this as the top-level skill when the user wants the full loop, not just one stage.

## Goal

Learn from historical bug-fix PRs and turn each PR into a small investigation cycle:

1. collect a seed PR set
2. extract the bugfix lesson from one PR
3. hunt similar suspect code on current HEAD
4. construct tests and confirm or reject the hypothesis

## Stage Order

1. `.agents/skills/historical-bugfix-seeding/SKILL.md`
2. `.agents/skills/bugfix-pr-briefing/SKILL.md`
3. `.agents/skills/similar-suspect-hunting/SKILL.md`
4. `.agents/skills/suspect-hypothesis-validation/SKILL.md`

## Operating Rules

- From Stage 2 onward, process bugfix PRs one by one.
- Keep each stage's artifact separate.
- Do not merge reasoning and local tooling. The agent thinks; the tool exports context, searches code, and assembles JSON.
- Do not file issues from Stage 4 blindly. Require current-HEAD evidence and a concrete failure signal.
- If a candidate is only fix code, only an import, only test code, or only naming similarity, reject it.

## Artifact Chain

- Stage 1: `artifacts/quality-enhance-loop/stage1_bugfix_seeds.json`
- Stage 2: `artifacts/quality-enhance-loop/stage2_bugfix_briefs.json`
- Stage 3: `artifacts/quality-enhance-loop/stage3_suspect_hypotheses.json`
- Stage 4: `artifacts/quality-enhance-loop/stage4_confirmed_bugs.json`

## Recommended Batch Size

- Stage 1: 10-40 seeds per run
- Stage 2: 1 PR at a time
- Stage 3: up to 3 suspects per seed PR
- Stage 4: validate suspects one by one

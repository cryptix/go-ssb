---
name: pr-awareness
description: "Fetch detailed PR reviews, CI status, and SonarQube findings. Use after push or on request — basic PR context is already injected by the SessionStart hook."
allowed_tools: ["Bash", "Read", "Grep", "Glob"]
---

# PR Awareness

Fetches detailed PR reviews, CI status, and SonarQube findings — without running local checks.

Basic PR context (status, CI pass/fail, review count) is already injected by the `SessionStart` hook at conversation start. This skill goes deeper: full review comments, SonarQube details, thread analysis.

## When to Activate

- **After push**: To refresh CI results and catch new reviews
- **On request**: When the user asks about PR status or review details

## Procedure

### Step 1: Detect PR

```bash
gh pr view --json number,title,state,author,baseRefName,headRefName,reviewDecision,additions,deletions,changedFiles,url 2>/dev/null
```

If no PR exists for the current branch, stop silently. Do not clutter the conversation.

### Step 2: Fetch PR context (parallel)

Run all in parallel using the PR number from Step 1:

```bash
# CI check status
gh pr checks {number} --json name,state,description,link

# Inline review comments (code-level)
gh api repos/{owner}/{repo}/pulls/{number}/comments --paginate

# Top-level reviews (approve / comment / changes-requested)
gh api repos/{owner}/{repo}/pulls/{number}/reviews --paginate

# Conversation comments (includes SonarQube bot)
gh api repos/{owner}/{repo}/issues/{number}/comments --paginate
```

### Step 3: Parse findings

**SonarQube** — From issue comments, find the bot comment (login: `sonarcloud[bot]` or Quality Gate badge). Extract:
- Quality Gate status (passed/failed)
- Failed conditions
- Coverage on New Code
- Duplication on New Code
- New issues count
- Dashboard link

**CI checks** — Parse pass/fail per check. Note any failures but do NOT fetch job logs — keep it lightweight.

**Human reviews** — For each review comment:
- Note the author, file, line, and body
- Identify unresolved threads (concern raised, no follow-up commit)
- Detect implicit change requests (questions phrased as requirements)
- Detect conditional approvals ("LGTM, but...")

### Review interpretation heuristics

This team's review style is conversational and informal:

| Pattern | Meaning |
|---------|---------|
| "what do you mean by X?" | Wants clarity in the code — rename, comment, or ticket |
| "can we use X please" | Polite but firm refactor request |
| "should we check/handle X?" | Missing error handling concern |
| "LGTM, but..." | Everything after "but" is a should-fix |

Read full threads: if the author agreed to make a change but there is no follow-up commit, the thread is **unresolved**.

### Step 4: Present summary

Output a **brief** summary — 2-3 lines, not a wall of text:

> **PR #{number}** ({reviewDecision}): {title}
> CI: {pass/fail summary} | SonarQube: {pass/fail} | Reviews: {count} ({unresolved} unresolved)

If there are must-fix or should-fix items from reviewers, list them in one bullet each.

## What this skill does NOT do

- **No local checks** — no linting, no test runs, no vet — keep it lightweight.
- **No deep analysis** — no reading referenced code to validate concerns — keep it lightweight.
- **No CI log fetching** — just pass/fail status — keep it lightweight.

This skill is awareness, not assessment.

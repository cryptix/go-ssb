#!/usr/bin/env bash
# SessionStart hook: fetch lightweight PR context.
# stdout is injected into Claude's conversation context automatically.
# Fails silently — outputs nothing if gh is unavailable or no PR exists.

set -euo pipefail

# --- PR awareness ---
pr_json=$(gh pr view --json number,title,state,reviewDecision,additions,deletions,changedFiles,url 2>/dev/null) || exit 0

number=$(echo "$pr_json" | jq -r '.number')
title=$(echo "$pr_json" | jq -r '.title')
state=$(echo "$pr_json" | jq -r '.state')
decision=$(echo "$pr_json" | jq -r '.reviewDecision // "NONE"')
adds=$(echo "$pr_json" | jq -r '.additions')
dels=$(echo "$pr_json" | jq -r '.deletions')
files=$(echo "$pr_json" | jq -r '.changedFiles')
url=$(echo "$pr_json" | jq -r '.url')

# CI checks — one-line pass/fail summary
ci_summary=$(gh pr checks "$number" --json name,state 2>/dev/null \
  | jq -r '[.[] | select(.state != "SUCCESS")] | if length == 0 then "all passing" else [.[].name] | join(", ") | "failing: " + . end' 2>/dev/null) || ci_summary="unknown"

# Review comments count
review_count=$(gh api "repos/{owner}/{repo}/pulls/${number}/comments" --jq 'length' 2>/dev/null) || review_count="?"

# SonarQube — extract quality gate from bot comment
sonar_status=$(gh api "repos/{owner}/{repo}/issues/${number}/comments" --jq '
  [.[] | select(.user.login == "sonarcloud[bot]")] | last | .body // ""
' 2>/dev/null | grep -oE 'Quality Gate (passed|failed)' | head -1) || sonar_status=""

echo "**PR #${number}** (${decision}): ${title}"
echo "State: ${state} | Size: +${adds} -${dels} (${files} files) | CI: ${ci_summary}"
[ -n "$sonar_status" ] && echo "SonarQube: ${sonar_status}"
echo "Reviews: ${review_count} inline comments"
echo "URL: ${url}"

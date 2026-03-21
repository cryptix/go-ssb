# Context Awareness — Conversation Start

## Automatic (via hook)

PR context is injected automatically by the `SessionStart` hook (`.claude/hooks/session-start-context.sh`). This runs deterministically every session — no action needed from you.

## Behavior

- Be **brief** — present a 2-3 line summary of PR status, not a wall of text
- If no PR is found, say nothing — do not clutter the conversation
- Do **not** run local checks (linting, tests) at conversation start — only on explicit request

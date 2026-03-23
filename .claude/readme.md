## Installation

1. Put `CLAUDE.md` into your repo root
2. Put the rest of the folder into your `.claude` / `.cursor` folder
3. Make sure `.claude/hooks/session-start-context.sh` is executable (`chmod +x`)
4. Merge `settings.json` into your existing `.claude/settings.json` (hooks section)

## Prerequisites

- **GitHub CLI (`gh`)** — used by the session-start hook for PR awareness. [Install instructions](https://cli.github.com/)

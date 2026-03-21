# Agent Orchestration

## Available Agents

Located in project root `.claude/agents/`:

### Reviewers (read-only, run in parallel)

| Agent                | Focus Area                                       |
|----------------------|--------------------------------------------------|
| go-reviewer          | Go idioms, error handling, concurrency, quality  |
| security-reviewer    | Injection, secrets, auth, OWASP vulnerabilities  |
| database-reviewer    | Data stores (Postgres, ClickHouse, margaret), queries, schema |
| api-reviewer         | API contracts, spec compliance, client UX        |
| p2p-test-reviewer    | Fixture-based node testing, replication, event streams |
| devops-reviewer      | Logging, monitoring, Docker, CI/CD, deployment   |

### Planners & Builders (run individually)

| Agent             | Purpose                 | When to Use                   |
|-------------------|-------------------------|-------------------------------|
| planner           | Implementation planning | Complex features, refactoring |
| architect         | System design           | Architectural decisions       |
| go-build-resolver | Fix build errors        | When build fails              |

## Review Workflow

After code changes, launch **all relevant reviewers in parallel**. They are read-only and cannot interfere with each other. Each reviewer reports findings independently.

Once all reviewers report back, synthesize their findings and apply fixes in the main conversation (or via go-build-resolver for build errors).

Not every reviewer needs to run every time — pick the ones relevant to what changed:

- Go code changed → go-reviewer + security-reviewer
- Data store code changed (SQL, queries, log operations) → database-reviewer
- API handlers or routes changed → api-reviewer + security-reviewer
- P2P node logic, replication, or event streams changed → p2p-test-reviewer
- Logging, Docker, CI, or config changed → devops-reviewer
- Large feature → all reviewers

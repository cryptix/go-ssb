# Always Use planner + architect Before Implementing Complex Features

**Extracted:** 2026-03-09
**Context:** Any request that involves new packages, architectural decisions,
or cross-cutting concerns.

## Problem
Skipping planner/architect agents leads to unilateral architectural decisions
made during implementation, which are harder to reverse and miss alternatives.

## Correct Sequence
1. Research agents (codebase + external docs) — parallel
2. architect agent → adapter choice, mounting strategy, interface boundaries
3. planner agent → file structure, task list, risk register
4. Implementation
5. go-reviewer → immediately after writing code

## Trigger Conditions
Use planner + architect when the task involves ANY of:
- New packages or sub-packages
- New external dependencies
- Versioning / API design decisions
- Cross-service or cross-package interfaces
- Middleware or authentication architecture

## Anti-pattern
"I have enough context from research to feel confident" → still use the agents.
Confidence is not a substitute for structured planning.

## When to Use
Every complex feature request in this project. No exceptions.

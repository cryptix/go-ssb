# Development Workflow

> This file extends [common/git-workflow.md](./git-workflow.md) with the full feature development process that happens before git operations.

The Feature Implementation Workflow describes the development pipeline: research, planning, TDD, code review, and then committing to git.

## Feature Implementation Workflow

0. **Research & Reuse** _(mandatory before any new implementation)_
   - **GitHub code search first:** Run `gh search repos` and `gh search code` to find existing implementations, templates, and patterns before writing anything new.
   - **Web search:** Use web search during the planning phase for broader research, data ingestion, and discovering prior art.
   - **Check package registries:** Search pkg.go.dev, and other registries before writing utility code. Prefer battle-tested libraries over hand-rolled solutions.
   - **Search for adaptable implementations:** Look for open-source projects that solve 80%+ of the problem and can be forked, ported, or wrapped.
   - Prefer adopting or porting a proven approach over writing net-new code when it meets the requirement.

1. **Plan First**
   - Use **planner** agent to create implementation plan
   - Generate planning docs before coding: PRD, architecture, system_design, tech_doc, task_list
   - Identify dependencies and risks
   - Break down into phases
2. **Code Review**
   - Use **go-reviewer** agent immediately after writing code
   - Address CRITICAL and HIGH issues
   - Fix MEDIUM issues when possible
3. **Database Review**
   - Use **database-reviewer** agent immediately after writing or modifying sql queries
   - Address CRITICAL and HIGH issues
   - Fix MEDIUM issues when possible
4. **Fix build errors**
   - use **go-build-resolver** agent immediately after a build/compile error
   - Address CRITICAL and HIGH issues
   - Fix MEDIUM issues when possible
5. **Commit**
   - Detailed commit messages
   - Follow conventional commits format
   - See [git-workflow.md](./git-workflow.md) for commit message format and PR process

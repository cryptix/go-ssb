---
name: go-rename
description: Rename Go identifiers using gopls language server for semantically-correct, cross-package renames. Use when the user asks to rename a Go type, function, method, variable, field, constant, package, or any other Go identifier. Much safer than string replacement because gopls understands scope, packages, interfaces, and embedded fields.
argument-hint: [old-name new-name]
allowed-tools: LSP, Bash, Read, Grep, Glob
---

# Go Rename — Semantic Identifier Rename via gopls

Rename Go identifiers using the `gopls rename` command for safe, semantically-aware renames across the entire workspace.

## Why gopls rename instead of Edit/string replacement

- **Scope-aware**: Only renames the actual symbol, not unrelated string matches
- **Cross-package**: Renames all references across packages in the module
- **Interface-aware**: Renames method implementations when renaming an interface method
- **Embedded field-aware**: Handles promoted fields and methods correctly
- **Import-aware**: Updates import paths when renaming packages
- **Comment-aware**: Updates doc comments that reference the symbol

## Procedure

### Step 1: Locate the identifier

Find the **definition site** of the identifier to rename. You need the exact file path, line number, and column number.

**If the user provides a name but not a location:**

1. Use `Grep` to search for the identifier name in `.go` files
2. Use `LSP goToDefinition` on any reference to find the canonical definition
3. Note the exact `file:line:column` of the definition

**If the user provides a file and line:**

1. Use `Read` to view the file and confirm the identifier
2. Determine the column (1-based byte offset from start of line to the first character of the identifier)

### Step 2: Preview the rename

Run `gopls rename` in diff mode to preview all changes before applying:

```bash
gopls rename -d <file>:<line>:<col> <NewName>
```

- `<file>` — absolute or module-relative path to the Go source file
- `<line>` — 1-based line number
- `<col>` — 1-based column (byte offset within the line)
- `<NewName>` — the new identifier name

Review the diff output:
- Verify only intended symbols are being renamed
- Check that the number of affected files is reasonable
- Confirm no unexpected cross-package changes

**If gopls rename fails**, check:
- The position points to a valid Go identifier
- `gopls` is installed and the module builds (`go build ./...`)
- The column number is correct (byte offset, not character offset)

### Step 3: Apply the rename

Once the preview looks correct, apply the rename:

```bash
gopls rename -w <file>:<line>:<col> <NewName>
```

The `-w` flag writes changes directly to the source files.

### Step 4: Verify

After applying the rename:

1. Run `go build ./...` to confirm the code compiles
2. If the build fails, check for:
   - Generated code that needs regeneration (protobuf, sqlc, mockery)
   - Build tags or files excluded from the rename
   - Vendored dependencies

### Step 5: Handle generated code

If the renamed identifier appears in generated files:

| Generator | Action |
|-----------|--------|
| protobuf (`gen/`) | Rename in `.proto` source, then `buf generate` |
| sqlc (`queries/`) | Rename in SQL query comments, then `sqlc generate` |
| mockery | Re-run `mockery` to regenerate mocks |

Do NOT manually edit generated files — regenerate them from their source.

## Examples

Rename a struct:
```bash
gopls rename -d internal/api/handler/user.go:15:6 CustomerHandler
gopls rename -w internal/api/handler/user.go:15:6 CustomerHandler
```

Rename a method:
```bash
gopls rename -d internal/keeper/service/user.go:42:20 FetchByEmail
gopls rename -w internal/keeper/service/user.go:42:20 FetchByEmail
```

Rename a package-level function:
```bash
gopls rename -d internal/handler/user.go:8:6 SanitizeInput
gopls rename -w internal/handler/user.go:8:6 SanitizeInput
```

## Edge Cases

- **Renaming interface methods**: gopls renames all implementations. Preview carefully — this can touch many files.
- **Renaming exported identifiers**: May break external consumers outside the module. Warn the user.
- **Renaming struct fields used in JSON tags**: gopls does NOT update struct tags. You must update `json:"..."` tags manually with Edit after the rename.
- **Renaming across build tags**: gopls may miss files behind build tags not active in the current environment.
- **Column calculation**: Count bytes from the start of the line (position 1), not characters. For ASCII identifiers this is the same, but matters for files with non-ASCII content before the identifier.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `gopls: not found` | Install: `go install golang.org/x/tools/gopls@latest` |
| `no identifier found` | Double-check line:col position — column is 1-based byte offset |
| `rename conflicts with existing` | The new name already exists in scope — choose a different name |
| `can't rename package` | Package renames may require updating `go.mod` and directory names manually |
| Rename is slow on large codebase | Normal for first run — gopls needs to index. Subsequent renames are faster |

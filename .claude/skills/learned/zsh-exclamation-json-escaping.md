# zsh Escapes `!` in Shell Arguments, Corrupting JSON

**Extracted:** 2026-03-09
**Context:** Passing JSON with `!` to curl or any shell command in zsh

## Problem

In zsh, `!` triggers history expansion even inside single-quoted strings and
`--data-raw` arguments when passed via the Bash tool. The `!` becomes `\!` in the
actual bytes, which causes `protojson.Unmarshal` (and standard JSON parsers) to fail:

```
proto: syntax error (line 1:41): invalid escape code "\!" in string
```

Symptoms:
- `--data-raw '{"password":"Test1234!"}'` → file contains `Test1234\!`
- `printf '%s' '...'` → same issue
- Position in error message points exactly to the `!` character

## Solution

Write the JSON body to a temp file using Python (bypasses all shell escaping), then
pass it to curl with `-d @file`:

```bash
python3 -c 'import json; open("/tmp/body.json","w").write(json.dumps({"email":"user@example.com","password":"Test1234!"}))'
curl -s -X POST -H "Content-Type: application/json" -d @/tmp/body.json http://localhost:8080/...
```

Verify with `od -c /tmp/body.json` before sending — no `\` should appear before `!`.

## When to Use

Whenever a curl command fails with a protojson parse error and the input contains `!`,
or any time JSON construction involving special shell characters (`!`, backticks, `$`)
is needed in a zsh Bash tool context.

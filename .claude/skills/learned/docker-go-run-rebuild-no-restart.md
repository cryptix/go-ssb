# `docker compose up --build` Does Not Restart Running `go run` Containers

**Extracted:** 2026-03-09
**Context:** Go services running via `go run .` in Docker Compose

## Problem

`docker compose up -d --build` (or `make dev-rebuild SVC=api`) rebuilds the Docker
image but does NOT restart containers that are already running if the image hash is
unchanged. For services using `go run .` as their entrypoint, the Go source is compiled
at container startup — so if the container is not restarted, it keeps running the old
compiled code even after source changes.

The container shows as "Running" in `docker ps` with the old uptime, while a freshly
started container would recompile.

## Solution

After any source code change, explicitly restart the service:

```bash
make dev-restart SVC=api        # restart one service
make dev-restart SVC="api,keeper"  # restart multiple
```

Or use the full rebuild+restart flow:

```bash
make dev-rebuild SVC=api   # rebuilds image
make dev-restart SVC=api   # forces container recreation
```

To verify which code is running, check the container uptime:
```bash
docker ps --format "{{.Names}}\t{{.Status}}" | grep api
```
A freshly restarted container shows "Up N seconds".

## When to Use

Any time a code change is not reflected in a running Docker Compose service, especially
for `go run .` entrypoint services where compilation happens at startup.

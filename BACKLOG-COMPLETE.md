# Completed backlog.

Finished tasks get pasted here from `BACKLOG.md`, with a `Completed: <date>, <commit SHA>` line added. Newest go at the bottom. Agents don't need to read this file to pick up work.

### 20261001-103222-1: Run Go tests in Docker.
- **Do:** Add `docker/test.Dockerfile` (`golang:1.27`, cgo on) and a `Makefile` with `test`, `test-unit`, and `shell` targets. Mount the Docker socket so testcontainers can start sibling containers.
- **Red test:** `TestHarnessRuns`. It fails until `make test` runs inside the container.
- **Done when:** `make test` passes on macOS and Linux.
- Completed: October 1, 2026, 4efbc1f. Built on macOS only. The Linux check is split out as 20261001-111132-1. Root-owned files on Linux are deferred to -4.

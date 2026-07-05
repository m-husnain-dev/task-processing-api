# Task API — Go REST API + Worker Pool + Tests

A small but production-shaped Go backend service. Built to demonstrate the
concepts most commonly tested in backend/systems interviews: concurrency
control, graceful shutdown, error handling, and table-driven tests.

## What it does

- `POST /tasks` — accepts a payload, immediately returns `202 Accepted` with
  a task ID, and queues the work for background processing.
- `GET /tasks/{id}` — check a task's current status (`pending` →
  `processing` → `done`/`failed`).
- `GET /tasks` — list all tasks.
- `GET /health` — health check.

## Why it's built this way

- **Worker pool, not one-goroutine-per-request.** A fixed number of
  goroutines (default 4) pull jobs from a buffered channel. This caps
  memory/CPU usage under load instead of letting a traffic spike spawn
  unbounded goroutines.
- **Backpressure.** If the job queue is full, `Submit` returns `false` and
  the API responds `503 Service Unavailable` instead of silently blocking
  or crashing.
- **Graceful shutdown.** On `SIGINT`/`SIGTERM` (Ctrl+C, `docker stop`, a
  Kubernetes pod eviction), the HTTP server stops accepting new
  connections, in-flight jobs are allowed to finish, then the process exits
  cleanly.
- **Thread-safe store.** An in-memory map protected by `sync.RWMutex` —
  many concurrent reads, exclusive writes. Verified race-free with
  `go test -race`.

## Running it

```bash
go run .
# server listens on :8080
```

```bash
curl -X POST localhost:8080/tasks -d '{"payload":"hello world"}'
# {"id":"t-1","payload":"hello world","status":"pending",...}

curl localhost:8080/tasks/t-1
# {"id":"t-1","status":"done","result":"processed[worker=1]: HELLO WORLD",...}
```

## Testing

```bash
go test -race -v ./...        # all tests, with the data-race detector on
go test -bench=. -benchmem    # benchmarks
go vet ./...                  # static analysis
```

Tests cover:
- Table-driven tests for the store (`TestStore_CreateAndGet`)
- Concurrent-access race testing (`TestStore_ConcurrentAccess`)
- Worker pool success + failure paths
- Backpressure when the queue is full
- HTTP handlers via `httptest`, no real network needed

## File layout

```
models.go       Task struct + request/response shapes
store.go        Thread-safe in-memory store (mutex + atomic ID counter)
worker.go       Worker pool: goroutines consuming a job channel
handlers.go     HTTP handlers (net/http, no framework)
main.go         Wiring + graceful shutdown
main_test.go    Unit + concurrency + HTTP tests
```

## Natural next steps (good talking points in interviews)

- Swap the in-memory `Store` for Postgres/Redis behind the same interface.
- Add a `Cancel(id)` endpoint that cancels a pending/processing job via
  per-task `context.WithCancel`.
- Add structured logging (`log/slog`) and request IDs via context.
- Rate-limit `POST /tasks` per client.

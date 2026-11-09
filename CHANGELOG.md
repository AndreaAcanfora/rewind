# Changelog

## Unreleased

- Python server (`python/`): ASGI app, `start` for Starlette/FastAPI, `MemoryStore` and a `RedisStore` compatible with the TypeScript one. Passes the TypeScript conformance suite in CI.

## 0.2.0 — 2026-11-09

- `RedisStore`: streams shared by every server instance, with `nodeRedis` and `ioredis` adapters. Cancellation works across instances, and a stream whose producer is gone is ended.
- Store contract tests run against `MemoryStore` and, in CI, against Redis 7.
- Releases are staged on npm with trusted publishing and provenance.

## 0.1.0 — 2026-11-09

- Resumable Stream Protocol specification, version 0.1 (draft).
- Server: Fetch API handler (SSE, snapshots, cancellation), `start`, `produce`, in-memory store, `node:http` adapter.
- Client: `startStream`, `readStream` (automatic resume, duplicate and gap handling, backoff), `fetchSnapshot`.
- Conformance suite and `rewind-conform` command.
- Demo chat (fake model or Claude) and a browser end-to-end test.

# Changelog

## 0.1.0 — 2026-11-09

- Resumable Stream Protocol specification, version 0.1 (draft).
- Server: Fetch API handler (SSE, snapshots, cancellation), `start`, `produce`, in-memory store, `node:http` adapter.
- Client: `startStream`, `readStream` (automatic resume, duplicate and gap handling, backoff), `fetchSnapshot`.
- Conformance suite and `rewind-conform` command.
- Demo chat (fake model or Claude) and a browser end-to-end test.

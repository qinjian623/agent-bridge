> Language: English | [中文](zh/multi-machine-review.md)

# Multi-Machine Design Adversarial Review Record (Historical Archive)

> **Archive Note**: All 16 review findings (`A1-A8` and `D1-D8`) recorded here have been integrated into the canonical specification:
> 👉 **[Multi-Machine Access Design & Architecture Specification](multi-machine-design.md)** (Section 3: Architectural Decision Records).
> Retained for historical auditability of the static adversarial review.

---

## 1. Review Scope and Subjects

- **Date**: 2026-11-03
- **Targets**: Initial draft of `docs/multi-machine-design.md` (SHA-256: `ddef0b4617a8297962f0480be377d4742fb2fd45b167b3b2f6dc64f5b6c47f1a`) and single-host core source files.
- **Method**: Two independent reviews executed via local `cli-agent-bridge` using isolated state directories and file snapshots without modifying production config or sessions.

| Reviewer | Runtime / Provider | Requested & Reported Model | Outcome |
|---|---|---|---|
| **AGY** | Antigravity / `google-antigravity` | `gemini-4-argon-high` | `completed`: 8 findings (5 P1, 3 P2) |
| **DeepSeek** | Pi / `deepseek` | `deepseek-flash` | `completed`: 8 findings (6 P1, 2 P2) |

---

## 2. Consolidated Findings and Design Resolutions

The 16 findings across both reviews were synthesized into concrete architectural remedies:

| # | Finding Ref | Core Issue Identified | Resolution in Canonical Architecture |
|---|---|---|---|
| 1 | A1, D5, D6 | Reverse push incompatible with short-lived Unix sockets | Switched to coordinator-initiated polling via `peer.pull_requests` pulling node outbox; relay keeps short-lived local socket calls. |
| 2 | A2, D1 | Self-declared `native_id` allows sender spoofing | Introduced broker-issued short-lived opaque Binding validated against caller PID, `starttime`, and locks under controlled Member MCP. |
| 3 | A6, D2 | DB snapshot rollback silently regresses Generation | Added out-of-DB `node_identity.json` tracking generation and monotonic `max_confirmed_seq`; rollback triggers `state_corrupted` and halts writes. |
| 4 | D6 | Identical `request_id` collisions across hosts | Scoped global idempotency to `(node_id, state_generation, operation_id)`. |
| 5 | D3 | External native CLI writes cannot atomically join SQLite commits | Enforced two-phase intent/receipt state: persist intent -> mark `sending` -> run CLI -> write receipt; unconfirmed becomes `uncertain` without auto-retry. |
| 6 | A3 | `operation_pending` treated as conflict deadlock by local cache | Stored Operation references in `api_requests`; inspect remote Operation state across reconnects until terminal resolution. |
| 7 | A4, D8 | Remote members violate local group FKs / collide on UUIDs | Added separate `remote_group_members`, `peer_inbox`, and FK-free `native_send_queue` keyed by host/generation triples. |
| 8 | A5 | Later native messages overtake pending head-of-line items | Enforced strict FIFO per-target sequencing; `uncertain` head-of-line blocks followers until resolved via explicit admin Evidence. |
| 9 | D7 | Deleting un-dispatched queue items leaves permanent sequence gaps | Added `Cancel Tombstone` preserving original `target_seq` and acknowledged on target before advancing sequence counters. |
| 10 | A7, D4 | Remote `event_seq` collides with coordinator FK constraints | Added `parent_ref` mapping and coordinator deduplication mirroring remote events into local `events.seq` for `deliveries`/`parent_ack`. |
| 11 | A8 | Mixed local `cwd` vs remote targets & missing cancel keys | Decoupled caller source `cwd` from remote `project_id`; expanded `agent_cancel` for `operation_id` and `delivery_id`. |
| 12 | D5, D8 | Relay lacked strict method allowlist | Locked relay dispatch behind strict `peer.*` allowlist forbidding arbitrary commands or remote quota tampering. |
| 13 | A3 | Split remote `open`+`dispatch` leaks orphan sessions on disconnect | Combined remote `open`+`dispatch` into one atomic compound Operation when `session_id` is omitted. |
| 14 | Coordinator | Shared accounts bypass cross-host budgets | Scoped `token_budget` strictly per host (`budget_scope=host`); forbid metered broadcasts including remote native members. |
| 15 | Coordinator | Large outputs bloat cross-host notification messages | Transferred compact `ArtifactRef` (`host_id + artifact_id + sha256`) and paginated large payloads on demand. |
| 16 | Coordinator | Reconnect replays reset confirmed status backward | Kept durable monotonic cursors and guarded confirmed receipt/processing states against stale downgrades. |

---

## 3. Private Review Artifacts

Raw review outputs, task/attempt IDs, hashes, and teardown logs remain stored locally in private state:
`<private-report-04>/`

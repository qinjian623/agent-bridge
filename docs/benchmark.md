> Language: English | [中文](zh/benchmark.md)

# Fixed-Task Benchmark: 2026-10-22

Three configurations each first modified the same string-normalization function; all three passed the fixed assertions. Two held-out tasks ("merge intervals" and "chunked iterator")—excluded from sample calibration—were then compared across three execution modes. Every run created an isolated Git worktree from the same commit; the primary session ran `gpt-6-astra / low`. Explicit delegation assigned tasks to Codex Sol and Pi DeepSeek; balanced auto-selection chose Pi DeepSeek on both held-out validation tasks.

| Execution Mode | Fixed Assertions & Main-Session Review Passed | Total Tokens (2 Tasks) | Median Duration | Slower Task Duration |
| --- | --- | ---: | ---: | ---: |
| Astra direct edit | 2/2 | 186,296 | 29.2s | 31.1s |
| Explicit runtime delegation | 2/2 | 180,853 | 29.4s | 38.7s |
| Balanced auto-selection | 2/2 | 125,213 | 19.7s | 21.1s |

Token counts include both the parent session and all child task attempts in each case; parent-session turns for writing delegation instructions and inspecting patches are included. Each parent record's cumulative token delta matched the sum of its native turn usages. Timing starts before workspace creation and stops after patch inspection, session closure, and usage metering stop. No failures or retries occurred in this run; retaining failure/retry overhead is tested separately in automated tests.

Calibration samples were three independent runs excluded from the held-out comparison table above; initial calibration incurs its own one-time token cost:

| Child Configuration | Total Tokens (with Astra Parent) | Total Duration | Outcome |
| --- | ---: | ---: | --- |
| Codex Sol | 143,280 | 37.9s | Pass |
| Pi DeepSeek | 59,834 | 23.8s | Pass |
| Claude Code / DeepSeek | 78,300 | 19.9s | Pass |

The benchmark script is `scripts/benchmark.py`. Held-out tasks set `learn_from_result=false` so they did not influence selection weights; execution order was varied to reduce order bias. Each case used a fresh native session, though provider-side prompt caches were neither cleared nor controlled (cached tokens are recorded in the raw report). Because only two small coding tasks ran once each, general savings cannot be extrapolated from this sample. Tokens are not currency, and without complete comparable billing rates across all configurations, no dollar cost-savings percentage is claimed.

Private full report: `<private-report-01>/validation.json` (contains raw responses, patches, test outputs, native usages, and session IDs; excluded from version control). The three calibration runs plus six validation runs totaled 16 parent-session turns and 7 child attempts, all completing normally.

All 9 parent records used `notify=false` and left the delivery table empty. All 12 Codex and Claude native sessions were registered as external child sessions and verified to produce 0 bot outbox entries after a scan cycle. Pi used private RPC session files. Test workers and the temporary service exited cleanly.

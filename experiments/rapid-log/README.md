# Historical Rapid append-log prototype benchmark

The prototype has been replaced by the [production implementation](../../docs/rapid-log-persistence.md). Its separate code and test harness were removed. The original storage-only measurements below are retained for comparison; they do not measure actor RPC latency. Production benchmarks use [the lifecycle runners](../../tests/gke-lifecycle/README.md).

## Measured on GKE, 2026-09-30

The live test passed on a gVisor pod in `us-west4-a`, with a 500m CPU limit and 256 MiB memory. Rapid replicas were in `us-west4-a` and `us-west4-b`; Standard was regional `us-west4`. This used the unoptimized Rust test profile, 228-byte state payloads (276 bytes including log framing), and 100 measured samples per row after five warmups. The three main modes alternated execution order; the additional append-only run followed them. The snapshot race used its existing open/finalize/move path without pre-opened writers.

| Credentials | Persistence path | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|
| Actor scoped | Two-zone append/flush | 3.24 | 4.43 | 5.02 |
| Actor scoped | Snapshot race | 155.87 | 186.73 | 214.72 |
| Actor scoped | Standard only | 197.02 | 212.49 | 237.88 |
| Actor scoped | Append/flush back to back | 2.87 | 4.82 | 43.73 |
| Workload identity | Two-zone append/flush | 4.24 | 6.33 | 9.02 |
| Workload identity | Snapshot race | 57.35 | 69.37 | 71.91 |
| Workload identity | Standard only | 56.45 | 68.54 | 72.73 |
| Workload identity | Append/flush back to back | 3.23 | 4.25 | 35.04 |

Actor-scoped append/flush reduced median warm persistence latency by 97.9% versus the snapshot race (48.2 times faster). Both zones confirmed the persisted offset before each acknowledgment. Back-to-back runs still had tail outliers; these measurements do not establish a latency guarantee or sustained-throughput limit.

One initial-open and one recovery observation were recorded per credential mode:

| Credentials | Initial two-stream open, ms | Fence/read/reseed recovery, ms |
|---|---:|---:|
| Actor scoped | 130.77 | 146.07 |
| Workload identity | 271.28 | 132.66 |

These are single observations, not cold/resume latency distributions. Credential issuance occurred before the timed open. Recovery included reading 210 records (57,960 bytes per replica) and flushing the recovered state to two fresh streams. Checkpointing was verified separately and excluded from the recovery timer. Both credential modes passed real-GCS checks for rejecting the old writer, preserving state after takeover, continuing versions, and ignoring an incomplete persisted tail.

The local library suite passed 318 tests, with 16 opt-in tests ignored; the live GKE benchmark passed separately and cleaned up its objects. Exact metrics, measured samples, build ID, and immutable image digest are in [benchmark-2026-09-30.json](benchmark-2026-09-30.json).


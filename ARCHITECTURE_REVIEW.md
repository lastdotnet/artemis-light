# Architecture Review — artemis-light

**Date:** 2026-02-23
**Branch:** chore/architecture-review

## Overview

artemis-light is a modernized fork of the Artemis MEV framework — a Rust library for
detecting and executing blockchain (MEV) opportunities. It implements a three-stage pipeline:

```
Collectors ──→ [Event Channel] ──→ Strategies ──→ [Action Channel] ──→ Executors
(ingest)                           (compute)                          (execute)
```

**Stack:** Rust 2024 edition (nightly), Tokio async runtime, Alloy 1.0 for Ethereum,
broadcast channels for inter-component communication.

~1,067 LOC across 16 Rust source files.

---

## Critical Bugs

### 1. Panic in hot path — `engine.rs:150` ✅ FIXED

```rust
let mut event_stream = collector.get_event_stream().await.unwrap(); // PANIC
```

Every other component logs errors and continues. A single collector failing here crashes the
entire engine. Fixed as part of the engine.rs rewrite — now uses `match` with error logging.

### 2. Division by zero — `mempool_executor.rs:57` ✅ FIXED

```rust
let breakeven_gas_price = gas_bid_info.total_profit / gas_usage as u128;
```

If `estimate_gas()` returns 0, this panics. No validation on the gas estimate before dividing.

### 3. Startup race condition — `engine.rs:97-158` ✅ FIXED

The engine spawns components in this order: executors → strategies → collectors. But broadcast
channels don't buffer — events sent before all strategies subscribe are silently dropped. The
initial receivers from `broadcast::channel()` are immediately discarded (`_`).

---

## High Severity Issues

### 4. No shutdown mechanism ✅ FIXED

All spawned tasks run infinite `loop {}` blocks with no `CancellationToken`, no shutdown
signal, and no way to stop them.

### 5. Inconsistent error handling ✅ FIXED

All components now uniformly log errors via `tracing` and continue:
- Engine: collectors, strategies, and executors all use `error!()` on failure
- EventCollector: now logs `warn!()` on decode errors (was silently dropping with `.ok()`)
- LogCollector: removed no-op `filter_map(Some)`

### 6. No timeouts on RPC calls ✅ FIXED

`MempoolCollector` and `MempoolExecutor` now wrap all RPC calls with
`tokio::time::timeout()` (default 10s, configurable via `with_rpc_timeout()`).

### 7. Sequential mempool tx lookups — `mempool_collector.rs:35-42` ✅ FIXED

Each pending transaction hash is fetched sequentially via `filter_map`. On a busy mempool,
this creates a bottleneck.

### 8. Anti-pattern in test — `tests/main.rs:180-204` ✅ FIXED

`test_complete_flow` spawns a `std::thread` and creates a new Tokio runtime inside it.
Multiple runtimes sharing resources is a known source of deadlocks.

---

## Medium Severity Issues

### 9. Unnecessary clone per event — `types.rs:89-91`

In `FilterCollectorMap`, the closure `f` is cloned for every event processed.

### 10. Overly restrictive trait bounds — `engine.rs:74-76`

`Clone` is required on E and A but broadcast channels require it, so this is actually
necessary given the current channel choice. Switching to `mpsc` channels would remove
this restriction.

### 11. No backpressure handling

Broadcast channels drop old messages when full for slow receivers. No logging or detection
when this happens.

### 12. Duplicate test helpers ✅ FIXED

`TestCollector` is defined identically in both `#[cfg(doctest)]` and `#[cfg(test)]` blocks
in `collector_ext.rs`.

---

## Testing Gaps

| What's tested                              | What's NOT tested                            |
|--------------------------------------------|----------------------------------------------|
| BlockCollector happy path                  | Engine orchestration logic (0 tests)         |
| MempoolCollector happy path                | Error/failure paths for any component        |
| MempoolExecutor happy path                 | Channel overflow / backpressure              |
| Full pipeline (collector→strategy→executor)| Multiple concurrent strategies/executors     |
| CollectorExt combinators (5 tests)         | Edge cases: empty streams, rapid reconnect   |
| Instrumentation wrappers (2 tests)         | LogCollector, EventCollector (no tests)      |

Missing entirely: negative testing, stress/load tests, benchmarks, property-based tests.

---

## Documentation Gaps

- README is 14 lines — no getting started guide, no usage examples
- No `/examples` directory
- Half of public types lack doc comments
- Inline comment density is ~2%
- No documented error handling strategy or trust boundary documentation

---

## Security Posture

| Category         | Rating        | Notes                                           |
|------------------|---------------|--------------------------------------------------|
| Code safety      | **Excellent** | Zero `unsafe` blocks                             |
| Dependencies     | **Good**      | All pinned in Cargo.lock, current versions       |
| Crypto           | **Good**      | Industry-standard via alloy                      |
| Secrets          | **Excellent** | No hardcoded keys                                |
| Input validation | **Medium**    | Delegated entirely to upstream                   |
| DoS resilience   | **Medium**    | RPC timeouts added; no rate limiting or circuit breakers |
| Error recovery   | **Good**      | Consistent error logging, graceful shutdown via CancellationToken |

---

## Low Severity / Code Quality

| Issue                    | Location               | Notes                              |
|--------------------------|------------------------|------------------------------------|
| Stale comment `AWK:`     | `types.rs:30`          | Should be `TODO:` or removed       |
| Lost error context       | `mempool_executor.rs`  | `map_err` discards original chain  |
| Empty `migrations/` dir  | root                   | Unused                             |
| Doctests marked `#[ignore]` | `collector_ext.rs`  | Examples exist but never run       |

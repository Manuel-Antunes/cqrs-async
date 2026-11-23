# Benchmark baseline for the library migration

Recorded on the machine the migration runs on, at commit 18839c6 (Phase 0.4,
code identical to the starting point except formatting), as the plan's
"Portão de benchmark" requires. Later gate runs are compared with this file,
not with the numbers in `performance.md`.

Setup: Apple M4 Pro, PostgreSQL 17 in Docker (`cqrs-poc-pg`, port 55432),
release build, 3 runs, median. **Load average was 8 to 21 during the run**
(other processes on the machine), so the plan's ~20% noise applies.

```text
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run --release -p cqrs-versus -- 3
cargo bench -p cqrs-benchmarks --bench micro
```

## cqrs-versus

| Scenario | Unit | cqrs (ours) | cqrs-es 0.5 | ours vs theirs |
|---|---|---:|---:|---:|
| open account (new aggregate) | cmd/s | 4208 | 3206 | 1.31× |
| open account: p99 | ms | 16.30 | 16.71 | 0.98× |
| mixed (deposit/withdraw/transfer), 1k accounts | cmd/s | 4064 | 2744 | 1.48× |
| mixed: p50 | ms | 7.48 | 9.76 | 0.77× |
| mixed: p99 | ms | 17.46 | 24.48 | 0.71× |
| mixed: conflicts retried | per 1k cmds | 23.60 | 12.20 | 1.93× |
| mixed: refused by the rules | cmds | 0.00 | 0.00 | — |
| mixed: done, read model and transfers included | ms | 5002 | 7290 | 0.69× |
| hot account (32 writers, one account) | cmd/s | 126 | 91.09 | 1.38× |
| hot account: p99 | ms | 1271 | 2329 | 0.55× |
| hot account: conflicts retried | per cmd | 20.34 | 28.67 | 0.71× |
| hot account: done, read model included | ms | 15996 | 21957 | 0.73× |
| read model: accounts wrong | accounts | 0.00 | 15.00 | — |
| read model: money lost | units | 0.00 | 1091 | — |
| read model: events lost | events | 0.00 | 183 | — |
| read model rebuild | events/s | 39727 | 1714 | 23.18× |
| read model after rebuild: accounts wrong | accounts | 0.00 | 0.00 | — |
| 5000-event account, full replay: p50 | ms | 4.21 | 4.94 | 0.85× |
| 5000-event account, full replay: p99 | ms | 6.66 | 7.53 | 0.88× |
| 5000-event account, snapshot every 100: p50 | ms | 1.18 | 1.92 | 0.61× |
| 5000-event account, snapshot every 100: p99 | ms | 3.47 | 6.11 | 0.57× |

median of 3 runs; 32 clients and 32 connections per system

## Gate measures on this baseline

| Measure | plan base | this machine, baseline | gate passes if |
|---|---:|---:|---|
| open account, ours / theirs | 1.38x | 1.31x | >= 1.10x |
| mixed load, cmd/s | 1.57x | 1.48x | >= 1.25x |
| mixed load, p99 | 0.63x | 0.71x | <= 0.76x |
| mixed load, all done | 0.64x | 0.69x | <= 0.77x |
| hot account, cmd/s | 1.15x | 1.38x | >= 1.00x |
| read model rebuild | 20.8x | 23.18x | >= 16x |
| 5000-event replay, p50 | 0.79x | 0.85x | <= 0.95x |
| snapshot every 100, p50 | 0.51x | 0.61x | <= 0.61x |
| events or money lost | 0 | 0 | 0 |

Note: the snapshot p50 ratio of this baseline run already sits on the gate's
limit (0.61x). The absolute value is 1.18 ms against 1.92 ms for cqrs-es; the
ratio is sensitive to the competitor's own noise. A gate run that lands
slightly above 0.61x needs a second run before it is called a regression.

## micro (divan, median)

The plan quotes gateway 4.0 ns, four interceptors 15.6 ns and an event in the
projection 18.5 ns. On this run `unit/command_gateway` is 13.7 ns (it measures
the full gateway call, with the retry loop) and `command_intercepted_4`
13.3 ns. The plan's numbers and these may not measure the same benchmark
names; later runs are compared with the table below.

```text
micro                                 fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ aggregate                                        │               │               │               │         │
│  ╰─ record_events                                 │               │               │               │         │
│     ├─ 1                            82.61 ns      │ 20.87 µs      │ 165.6 ns      │ 470.8 ns      │ 100     │ 100
│     ├─ 100                          3.624 µs      │ 5.624 µs      │ 4.291 µs      │ 4.367 µs      │ 100     │ 100
│     ╰─ 10000                        360.2 µs      │ 560 µs        │ 378.9 µs      │ 387.5 µs      │ 100     │ 100
├─ dispatch                                         │               │               │               │         │
│  ├─ derived_gateway                 40.61 ns      │ 10.08 µs      │ 40.61 ns      │ 141.3 ns      │ 100     │ 100
│  ├─ direct_call                     0.374 ns      │ 0.649 ns      │ 0.384 ns      │ 0.386 ns      │ 100     │ 819200
│  ╰─ hlist_bus                       3.34 ns       │ 6.961 ns      │ 3.563 ns      │ 3.586 ns      │ 100     │ 204800
├─ interceptors                                     │               │               │               │         │
│  ├─ command_intercepted             3.543 ns      │ 8.528 ns      │ 3.604 ns      │ 3.678 ns      │ 100     │ 204800
│  ├─ command_intercepted_4           12.96 ns      │ 14.75 ns      │ 13.28 ns      │ 13.33 ns      │ 100     │ 51200
│  ├─ command_plain                   3.218 ns      │ 3.543 ns      │ 3.319 ns      │ 3.318 ns      │ 100     │ 204800
│  ├─ event_intercepted               11.25 ns      │ 12.8 ns       │ 11.49 ns      │ 11.53 ns      │ 100     │ 51200
│  ├─ event_measured                  48.11 ns      │ 54.96 ns      │ 50.07 ns      │ 50.34 ns      │ 100     │ 12800
│  ╰─ event_plain                     13.45 ns      │ 17.11 ns      │ 15.89 ns      │ 15.58 ns      │ 100     │ 51200
├─ persistence                                      │               │               │               │         │
│  ├─ add_new_aggregate               374.6 ns      │ 6.583 µs      │ 458.1 ns      │ 738.7 ns      │ 100     │ 100
│  ├─ command_cycle                   1.062 µs      │ 1.416 µs      │ 1.108 µs      │ 1.124 µs      │ 100     │ 400
│  ├─ load_and_replay                               │               │               │               │         │
│  │  ├─ 10                           697.4 ns      │ 749.6 ns      │ 713.1 ns      │ 715.4 ns      │ 100     │ 800
│  │  ├─ 1000                         56.41 µs      │ 127.4 µs      │ 59.41 µs      │ 59.95 µs      │ 100     │ 100
│  │  ╰─ 100000                       5.424 ms      │ 6.842 ms      │ 5.816 ms      │ 5.833 ms      │ 100     │ 100
│  ├─ load_with_snapshot_100k_events  108.9 ns      │ 163.6 ns      │ 122.6 ns      │ 121.4 ns      │ 100     │ 3200
│  ╰─ read_all_page                   9.457 µs      │ 17.24 µs      │ 9.499 µs      │ 9.646 µs      │ 100     │ 100
╰─ unit                                             │               │               │               │         │
   ├─ command_gateway                 13.36 ns      │ 17.68 ns      │ 13.69 ns      │ 13.75 ns      │ 100     │ 51200
   ╰─ command_through_unit            13.53 ns      │ 47.3 ns       │ 13.77 ns      │ 14.44 ns      │ 100     │ 51200
```

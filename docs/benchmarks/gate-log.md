# Benchmark gate log

`cqrs-versus`, ours / theirs (cqrs-es), median of 3 runs each. Absolute numbers of
ours in parentheses (cmd/s, ms or events/s). Same machine, Apple M4 Pro, PostgreSQL 17 in
Docker. Load average was 5-21 from unrelated processes during these runs (see below).

| Measure | gate | baseline (0.5) | phase 1 | phase 2, run a | phase 2, run b | A/B base 1 | A/B phase 2, 1 | A/B base 2 | A/B phase 2, 2 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| open account, cmd/s | >= 1.10 | 1.31 (4208) | 1.34 (4299) | 1.16 (4073) | 1.02 (3541) | 1.08 (3719) | 1.28 (3915) | 0.93 (3076) | 1.16 (3079) |
| mixed load, cmd/s | >= 1.25 | 1.48 (4064) | 1.52 (3928) | 1.29 (3758) | 1.07 (3011) | 1.23 (3379) | 1.34 (3538) | 0.93 (2411) | 1.29 (3142) |
| mixed load, p99 | <= 0.76 | 0.71 (17.46) | 0.71 (18.69) | 0.83 (18.85) | 0.97 (24.34) | 0.92 (22.94) | 0.81 (22.26) | 1.13 (34.63) | 0.81 (24.65) |
| mixed load, all done | <= 0.77 | 0.69 (5002) | 0.67 (5199) | 0.78 (5400) | 0.95 (6742) | 0.83 (6000) | 0.76 (5777) | 1.09 (8403) | 0.79 (6440) |
| hot account, cmd/s | >= 1.00 | 1.38 (126) | 1.24 (114) | 1.32 (133) | 1.23 (119) | 1.28 (126) | 1.29 (129) | 1.22 (88.72) | 1.25 (126) |
| read model rebuild | >= 16 | 23.18 (39727) | 25.53 (36641) | 27.73 (40437) | 23.19 (33434) | 25.68 (36468) | 25.56 (36960) | 27.94 (26680) | 25.25 (34687) |
| 5000-event replay, p50 | <= 0.95 | 0.85 (4.21) | 0.78 (4.46) | 0.81 (4.53) | 0.78 (4.36) | 0.76 (4.44) | 0.79 (4.49) | 0.79 (5.01) | 0.76 (4.63) |
| snapshot every 100, p50 | <= 0.61 | 0.61 (1.18) | 0.65 (1.29) | 0.43 (1.31) | 0.52 (1.29) | 0.62 (1.33) | 0.47 (1.39) | 0.34 (1.68) | 0.31 (1.35) |

## Reading it

- The **baseline** row passes the plan's thresholds on 8 of 8 measures (snapshot p50 exactly on its
  limit). The machine was not quiet then either (load average 8-21, `baseline.md`); that run simply
  fell on the better side of the noise.
- **Phase 1** (1.1-1.4): all measures pass except snapshot p50 (0.65x against 0.61x; baseline 0.61x).
- **Phase 2**, two plain gate runs a few minutes apart, same code: run a fails two measures
  (mixed p99 0.83x, mixed all-done 0.78x), run b fails four (open account 1.02x, mixed 1.07x, p99 0.97x,
  all-done 0.95x). The code did not change between them.
- So the gate cannot decide on this machine. To separate noise from regression, the Phase 0 code and
  the Phase 2 code were run **alternately, side by side** (A/B rows): under the same load the Phase 0
  code itself fails 4 of 8 measures in both rounds, and the Phase 2 code is equal or better in absolute
  throughput in both rounds (open account 3719/3076 vs 3915/3079; mixed 3379/2411 vs 3538/3142; hot account
  126/89 vs 129/126; rebuild 36468/26680 vs 36960/34687 events/s). p99 and the "all done" time follow
  the load the same way in both.
- Verdict: **no regression measured against the Phase 0 code**. The plan's thresholds were missed by
  both codes under this machine's load; applying the gate as written needs a quiet machine. Rows are
  kept so a later run on one can be compared.

A PostgreSQL container stop (the Docker daemon restarted; `cqrs-poc-pg` has no restart policy) killed one
gate run in the middle; it was discarded and rerun after `docker start cqrs-poc-pg`.

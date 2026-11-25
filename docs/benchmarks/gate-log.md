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

## Phase 3 (publisher in the handler)

Same method: the Phase 0 and Phase 3 release binaries run alternately (2 rounds), plus the micro
benchmarks. Load average 8-17.

| Measure | gate | base 1 | phase 3, 1 | base 2 | phase 3, 2 |
|---|---|---:|---:|---:|---:|
| open account, cmd/s | >= 1.10 | 1.32 (4008) | 1.07 (3682) | 0.71 (2021) | 1.14 (3606) |
| mixed load, cmd/s | >= 1.25 | 1.62 (3908) | 1.35 (3650) | 0.92 (2260) | 1.49 (2788) |
| mixed load, p99 | <= 0.76 | 0.52 (18.47) | 0.78 (20.82) | 2.66 (87.63) | 0.31 (34.48) |
| mixed load, all done | <= 0.77 | 0.63 (5220) | 0.75 (5532) | 1.15 (9304) | 0.68 (7220) |
| hot account, cmd/s | >= 1.00 | 1.47 (132) | 1.67 (136) | 1.41 (98.72) | 1.23 (119) |
| read model rebuild | >= 16 | 23.28 (37590) | 30.84 (39077) | 53.22 (29591) | 22.98 (30957) |
| 5000-event replay, p50 | <= 0.95 | 0.86 (4.28) | 0.88 (5.02) | 0.90 (5.38) | 0.72 (4.27) |
| snapshot every 100, p50 | <= 0.61 | 0.24 (1.13) | 0.32 (1.47) | 0.32 (1.76) | 0.48 (1.65) |

Base 2 is an outlier (2021 cmd/s open account, p99 88 ms: something else was using the machine). In
round 1 the Phase 3 binary is 7-8% lower on open account and mixed throughput and equal on the hot
account; in round 2 it is the better of the two. No consistent difference. Every gate measure passes
in round 1 except mixed p99 (0.78x against 0.76x) and the base binary passes it there by luck of the
load.

Micro benchmarks (median): `unit/command_gateway` 13.1 ns (Phase 0: 13.7), `command_through_unit`
13.0 (13.8), `command_intercepted_4` 11.8 (13.3), `command_cycle` 1.03 us (1.11), `add_new_aggregate`
375 ns (458). The static dispatch the plan wants kept costs nothing more after moving the publisher
into the handler. (These are not the plan's 4.0 / 15.6 ns: the plan's numbers do not reproduce on this
machine even for the Phase 0 code.)

## After the review fixes (commit 4662e11)

Snapshot-store failures logged, `tracing` without default features, dead letters as one trait over
the unit of work. Two plain gate runs back to back, median of 3 each. The machine was in use: load
average 10-11 during run a, 12-17 during run b.

| Measure | gate | run a | run b |
|---|---|---:|---:|
| open account, cmd/s | >= 1.10 | 0.95 (3265) | 1.10 (3040) |
| mixed load, cmd/s | >= 1.25 | 1.30 (3316) | 1.12 (2603) |
| mixed load, p99 | <= 0.76 | 0.69 (24.45) | 0.86 (34.25) |
| mixed load, all done | <= 0.77 | 0.78 (6111) | 0.91 (7797) |
| hot account, cmd/s | >= 1.00 | 1.10 (103) | 1.29 (105) |
| read model rebuild | >= 16 | 39.99 (41624) | 20.89 (26373) |
| 5000-event replay, p50 | <= 0.95 | 0.85 (4.60) | 0.79 (4.52) |
| snapshot every 100, p50 | <= 0.61 | 0.54 (1.23) | 0.36 (1.71) |

Events and money lost by our read model: 0 in both runs (cqrs-es: 171 and 254 events).

Run a misses two measures (open account, all done by 0.01), run b misses three (mixed throughput,
p99, all done). Only "all done" is missed in both, and the Phase 0 code missed it too under load
(A/B rows above). Throughput of both libraries fell from run a to run b as the load rose. The gate
is still undecided on this machine; none of the three commits touches the write path.

Micro benchmarks (median): `unit/command_gateway` 12.7 ns, `command_through_unit` 12.4,
`command_intercepted_4` 10.8, `command_cycle` 1.01 us, `add_new_aggregate` 333 ns,
`dispatch/derived_gateway` 1.4 ns, `load_with_snapshot_100k_events` 116 ns. Equal to or slightly
better than the Phase 3 numbers above.

### A/B against the Phase 0 code (18839c6 vs 4662e11)

The two release binaries run alternately, three rounds of one run each. Load average 17-22 (the
machine was busier than in any earlier run). Ours / theirs, ours in parentheses.

| Measure | base 1 | head 1 | base 2 | head 2 | base 3 | head 3 |
|---|---:|---:|---:|---:|---:|---:|
| open account, cmd/s | 1.38 (3234) | 1.22 (2886) | 1.10 (3400) | 1.09 (2729) | 1.15 (2731) | 1.20 (4476) |
| mixed load, cmd/s | 1.24 (2408) | 1.43 (2465) | 1.30 (3440) | 1.28 (2366) | 1.32 (2572) | 1.32 (3885) |
| mixed load, p99 | 0.79 (35.69) | 0.68 (33.97) | 0.88 (30.58) | 0.76 (36.34) | 0.79 (34.08) | 0.74 (18.26) |
| mixed load, all done | 0.81 (8401) | 0.71 (8201) | 0.78 (5899) | 0.79 (8532) | 0.77 (7901) | 0.77 (5200) |
| hot account, cmd/s | 1.23 (93.55) | 1.14 (96.32) | 1.25 (115) | 1.18 (93.89) | 1.31 (97.74) | 1.23 (130) |
| read model rebuild | 27.69 (20891) | 13.25 (22091) | 44.70 (39412) | 28.60 (21157) | 15.33 (21494) | 22.28 (42646) |
| 5000-event replay, p50 | 0.75 (5.92) | 0.85 (4.21) | 0.71 (5.33) | 0.97 (7.06) | 0.79 (3.90) | 0.81 (4.07) |
| snapshot every 100, p50 | 0.35 (2.27) | 0.33 (1.13) | 0.39 (1.88) | 0.21 (1.38) | 0.60 (1.09) | 0.55 (1.20) |

Median ratio, base then head: open account 1.15 / 1.20, mixed 1.30 / 1.32, mixed p99 0.79 / 0.74,
all done 0.78 / 0.77, hot account 1.25 / 1.18. The same binary moves between 2700 and 4500 cmd/s on
open account with the machine's load, and cqrs-es moves with it. No difference between the Phase 0
code and the current one stands out of that spread.

## 2026-11-24, after Phases 5, 6 and 7 (`926476c`)

One versus run (median of 3), load average 11-15 while it ran.

| Measure | Passes if | Result |
|---|---|---:|
| open account, cmd/s | >= 1.10 | 1.05 (3840) |
| mixed load, cmd/s | >= 1.25 | 1.35 (3888) |
| mixed load, p99 | <= 0.76 | 0.76 |
| mixed load, all done | <= 0.77 | 0.75 |
| hot account, cmd/s | >= 1.00 | 1.35 |
| read model rebuild | >= 16 | 21.8 |
| 5000-event replay, p50 | <= 0.95 | 0.82 |
| with snapshot every 100, p50 | <= 0.61 | 0.53 |
| events or money lost | 0 | 0 (cqrs-es: 197 events, 1174 units) |

Eight of nine pass; open account misses by 0.05. On this machine that
ratio has ranged from 0.71 to 1.38 for the same code (the runs above), and
our 3840 cmd/s is among the highest absolute values recorded: cqrs-es was
fast in this run, not ours slow. Open account p99 was 38 ms against 14 ms.
Not decided: needs the A/B against `5437e6e` on a quiet machine (the
append path changed in 7.4: AppendEvents with a transaction argument).

## 2026-11-24, after the command's unit of work (7b.1b, `9f0e243`)

One versus run (median of 3), load average 8-11. The append path of a
command without a transaction (`Autocommit`, what the versus handler is)
changed: the handler records, the gateway appends.

| Measure | Passes if | Result | Run before (`926476c`) |
|---|---|---:|---:|
| open account, cmd/s | >= 1.10 | 1.15 (4345) | 1.05 (3840) |
| mixed load, cmd/s | >= 1.25 | 1.34 (3952) | 1.35 (3888) |
| mixed load, p99 | <= 0.76 | 0.81 (18.4 ms) | 0.76 (18.8 ms) |
| mixed load, all done | <= 0.77 | 0.76 | 0.75 |
| hot account, cmd/s | >= 1.00 | 1.24 | 1.35 |
| read model rebuild | >= 16 | 22.4 | 21.8 |
| 5000-event replay, p50 | <= 0.95 | 0.85 | 0.82 |
| with snapshot every 100, p50 | <= 0.61 | 0.62 (1.09 ms) | 0.53 (1.12 ms) |
| events or money lost | 0 | 0 (cqrs-es: 170 events, 972 units) | 0 |

Seven of nine pass. The two that miss do so on cqrs-es's side: our mixed
p99 (18.4 ms) and snapshot replay (1.09 ms) are as good as or better than
in the run before, theirs improved (22.6 against 24.8 ms; 1.76 against
2.10 ms). The snapshot replay does not go through a command at all. Open
account, which misses before, passes now, with our highest throughput so
far. Read as noise on a loaded machine: the A/B on a quiet one decides.

## 2026-11-24, A/B against `5437e6e` after 7b, D17 and D18 (`768ac7d`)

The two release binaries run alternately, three rounds of one run each
(base, head, base, head, base, head). Nothing else ran on the machine:
load average 2.8 before the first round, 10-13 during the runs (the
benchmark's own 32 clients per system and PostgreSQL).

| Measure | Passes if | base r1 | base r2 | base r3 | **base** | head r1 | head r2 | head r3 | **head** |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| open account, cmd/s | >= 1.10 | 1.41 (3887) | 1.28 (4113) | 1.22 (4118) | **1.28** | 1.24 (4578) | 1.11 (3890) | 1.18 (4393) | **1.18** |
| open account, p99 | | 0.70 (23.4) | 0.79 (17.7) | 0.90 (18.0) | **0.79** | 0.94 (15.2) | 1.83 (25.7) | 1.22 (16.8) | **1.22** |
| mixed load, cmd/s | >= 1.25 | 1.37 (3798) | 1.34 (3837) | 1.28 (3877) | **1.34** | 1.16 (3528) | 1.53 (3257) | 1.32 (3929) | **1.32** |
| mixed load, p99 | <= 0.76 | 0.82 (19.7) | 0.80 (18.9) | 0.81 (18.5) | **0.81** | 0.95 (21.8) | 0.33 (24.0) | 0.80 (18.4) | **0.80** |
| mixed load, all done | <= 0.77 | 0.75 | 0.76 | 0.80 | **0.76** | 0.88 | 0.66 | 0.77 | **0.77** |
| hot account, cmd/s | >= 1.00 | 0.80 | 1.29 | 1.31 | **1.29** | 1.41 | 1.15 | 1.21 | **1.21** |
| read model rebuild | >= 16 | 18.1 | 24.0 | 26.4 | **24.0** | 22.9 | 18.9 | 34.7 | **22.9** |
| 5000-event replay, p50 | <= 0.95 | 0.79 | 0.77 | 0.87 | **0.79** | 0.81 | 0.77 | 0.81 | **0.81** |
| with snapshot every 100, p50 | <= 0.61 | 0.62 | 0.35 | 0.64 | **0.62** | 0.65 | 0.61 | 0.33 | **0.61** |
| events or money lost (ours / cqrs-es events) | 0 | 0 / 195 | 0 / 179 | 0 / 197 | **0** | 0 / 200 | 0 / 208 | 0 / 211 | **0** |

Head and base are equal within the spread of their own rounds on every
measure. Mixed p99 misses its threshold on both, by the same amount (0.80
and 0.81): the threshold comes from a 0.63 this machine does not
reproduce, with the base code either. The open account p99 of 38 ms seen
before (`926476c`) did not come back: ours was 15-26 ms, as low as or lower
than the base's 18-23 ms; its ratio moves with cqrs-es's p99.

Micro benchmarks (median, load below 6): `command_cycle` 1.02 us,
`command_intercepted_4` 13.6 ns, `interceptors/command_plain` 3.4 ns.
`dispatch` now routes a query (`Echo`), commands always running in a unit
of work (D17). `unit/command_gateway` is 29.4 ns against 14.3 ns at
`5437e6e`; measured at `c75b727` (before D17) it was 33.1 ns, so the
difference is the command's unit of work (7b.1b: begin, append, commit,
announce, the retry loop), not D17 or D18. 15 ns per command against a
command of about 1 ms on PostgreSQL; the versus does not see it.

## 2026-11-24, PostgreSQL path optimizations (A/B against `263f8cd`)

Four changes, none of them to the semantics: a conflict found by the
version check returns the stream's version in the append's own statement
(before, every conflict read it again); the batch and the metadata are
serialized straight to JSON text (no `serde_json::Value`); an aggregate is
rebuilt through `LoadStream::replay`, which reads and decodes only the
sequence, type, version and payload, row by row (no metadata, no stream
ids, no list of envelopes); a page read probes the index only for the
positions the page does not hold.

Both release binaries alternately, three rounds of one run each, load
average 6.7-12.5 (the "after" rounds started under slightly more load).
Absolute values, ours:

| Measure | before r1 | r2 | r3 | **before** | after r1 | r2 | r3 | **after** | change | ratio before -> after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| open account, cmd/s | 4151 | 4542 | 4286 | **4286** | 4335 | 4522 | 4675 | **4522** | +6% | 1.14 -> 1.31 |
| mixed load, cmd/s | 3993 | 3874 | 4027 | **3993** | 3979 | 4160 | 4273 | **4160** | +4% | 1.31 -> 1.40 |
| mixed load, p99 ms | 18.6 | 18.8 | 17.7 | **18.6** | 15.8 | 15.1 | 14.5 | **15.1** | -19% | 0.81 -> 0.67 |
| mixed load, all done ms | 5100 | 5282 | 5075 | **5100** | 5125 | 4900 | 4734 | **4900** | -4% | 0.78 -> 0.72 |
| hot account, cmd/s | 147 | 131 | 128 | **131** | 157 | 158 | 170 | **158** | +21% | 1.38 -> 1.42 |
| hot account, p99 ms | 1067 | 1209 | 1187 | **1187** | 927 | 938 | 863 | **927** | -22% | 0.60 -> 0.53 |
| read model rebuild, events/s | 40393 | 37283 | 40890 | **40393** | 42248 | 43405 | 48423 | **43405** | +7% | 21.8 -> 24.9 |
| 5000-event replay, p50 ms | 4.23 | 4.26 | 4.07 | **4.23** | 2.47 | 2.54 | 2.47 | **2.47** | -42% | 0.80 -> 0.49 |
| 5000-event replay, p99 ms | 6.26 | 6.13 | 6.07 | **6.13** | 4.27 | 5.13 | 4.03 | **4.27** | -30% | 1.03 -> 0.67 |
| snapshot every 100, p50 ms | 1.27 | 1.23 | 1.08 | **1.23** | 1.07 | 1.10 | 1.07 | **1.07** | -13% | 0.62 -> 0.61 |
| events or money lost | 0 | 0 | 0 | **0** | 0 | 0 | 0 | **0** | | |

The replay, the mixed p99 and the hot account do not overlap between the
two sets of rounds. Every gate measure passes, mixed p99 (0.67 <= 0.76)
included, for the first time on this machine. Conflicts per 1000 mixed
commands are unchanged (23.3 / 23.6): they come from the transfer policy
running asynchronously against the clients' accounts, not from the store.

## 2026-11-24, before the first release (`c315a04` against `c02e963`)

What changed since the last gate: the public modules, the envelope without
per-event copies, the checklist of public types, the wake-up between
instances in the readers' loop (`committed_elsewhere`), dead letters for a
projection's namespaces, the packaging. Both release binaries alternately,
three rounds of one run each, each round started below load 5.

| Measure | before r1 | r2 | r3 | **before** | release r1 | r2 | r3 | **release** | ratio before -> release | gate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| open account, cmd/s | 4095 | 4313 | 4446 | **4313** | 4078 | 3989 | 4406 | **4078** | 1.19 -> 1.14 | >= 1.10 pass |
| mixed load, cmd/s | 4150 | 4363 | 4188 | **4188** | 4111 | 4032 | 4300 | **4111** | 1.38 -> 1.35 | >= 1.25 pass |
| mixed load, p99 ms | 14.2 | 13.5 | 14.3 | **14.2** | 15.3 | 15.6 | 13.9 | **15.3** | 0.65 -> 0.70 | <= 0.76 pass |
| mixed load, all done ms | 4900 | 4698 | 4876 | **4876** | 4924 | 5022 | 4769 | **4924** | 0.74 -> 0.76 | <= 0.77 pass |
| hot account, cmd/s | 164 | 169 | 177 | **169** | 179 | 176 | 170 | **176** | 1.46 -> 1.44 | >= 1.00 pass |
| read model rebuild, events/s | 34940 | 41116 | 49769 | **41116** | 36600 | 52171 | 50592 | **50592** | 21.1 -> 25.5 | >= 16 pass |
| 5000-event replay, p50 ms | 2.59 | 2.54 | 2.61 | **2.59** | 2.46 | 2.48 | 2.40 | **2.46** | 0.53 -> 0.49 | <= 0.95 pass |
| snapshot every 100, p50 ms | 1.18 | 1.04 | 1.20 | **1.18** | 1.08 | 1.29 | 1.03 | **1.08** | 0.62 -> 0.50 | <= 0.61 pass |
| events or money lost | 0 | 0 | 0 | **0** | 0 | 0 | 0 | **0** | | pass |

Every gate measure passes. The two versions are equal within the spread
of their own rounds: open account and mixed throughput are a few percent
lower in the medians but inside the other version's range; the rebuild and
the hot account's p99 are better. No regression from what came in since.

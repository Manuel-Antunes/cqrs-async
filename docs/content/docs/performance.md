---
title: "Performance"
sidebar_position: 70
---

## Against cqrs-es

`benchmarks/versus` runs the same bank on this library and on cqrs-es 0.5
with postgres-es 0.5, against the same PostgreSQL: open, deposit, withdraw
and transfer, one aggregate per account, a balance read model, and a
transfer policy crediting the target account. The business rules live in
one shared module (`bank.rs`); each library only wraps them, wired the way
its docs recommend. Here, the read model and the transfers are tracking
processors with 8 segments and batches of up to 100 events
(`CQRS_VERSUS_BATCH`, default 100); in cqrs-es they run inline after each
commit.

```text
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run --release -p cqrs-versus -- 3
```

32 clients and a 32-connection pool per system, a fresh schema per system,
the median of the runs.

### Current numbers

The run after Phase 10 (`docs/benchmarks/gate-log.md`, "after Phase 10"):
three rounds, median of the rounds, alternating with the code of the
release gate under the same load. Absolute values are ours; the ratio is
ours divided by cqrs-es's, with the threshold the gate holds it to.

| Measure | Ours | Ours / cqrs-es | Gate passes if |
|---|---:|---:|---|
| open account | 4086 cmd/s | 1.20 | ≥ 1.10 |
| mixed load (1k accounts) | 3716 cmd/s | 1.30 | ≥ 1.25 |
| mixed load, p99 | 17.7 ms | 0.69 | ≤ 0.76 |
| mixed load: done, read model and transfers included | 5500 ms | 0.78 | ≤ 0.77 |
| hot account (32 writers, one aggregate) | 153 cmd/s | 1.52 | ≥ 1.00 |
| hot account, p99 | 977 ms | 0.50 | |
| read model rebuild | 40431 events/s | 24.9 | ≥ 16 |
| 5000-event aggregate, full replay, p50 | 2.84 ms | 0.48 | ≤ 0.95 |
| 5000-event aggregate, snapshot every 100, p50 | 1.23 ms | 0.62 | ≤ 0.61 |
| events or money lost by the read model | 0 | | 0 |

The machine was busier than at the release gate (load 4.3-4.9). Two
measures miss their threshold by 0.01: the time to done, which the
release's code missed by more under the same load (0.86), and the
snapshot p50, whose rounds spread from 0.41 to 0.62. Against that code,
nothing regressed. At the release gate, on a quieter machine, every
measure passed (open account 1.14, mixed 1.35, p99 0.70, done 0.76, hot
account 1.44, rebuild 25.5, replay 0.49, snapshot 0.50). cqrs-es's read
model loses events in every run (about 170 in each): concurrent commands
on one account race in its inline view updates.

### How the numbers got here

Three A/B runs decide the current state, all in `gate-log.md`:

1. **After the command's unit of work, the gateways as handlers and the
   query updates in the projection** (head `768ac7d` against `5437e6e`, the
   code before those changes). Head and base are equal within the spread of their own
   rounds on every measure: the redesign costs nothing measurable on
   PostgreSQL. Mixed p99 missed its threshold on both codes by the same
   amount (0.80 and 0.81).
2. **PostgreSQL path optimizations** (against `263f8cd`), four changes, none
   to the semantics: a conflict returns the stream's version in the append's
   own statement (no second read); the batch and its metadata are serialized
   straight to JSON text (no `serde_json::Value`); an aggregate is rebuilt
   through `LoadStream::replay`, which reads and decodes only what
   rebuilding uses; a page read probes the index only for the positions the
   page does not hold. Replay p50 went from 4.23 to 2.47 ms (-42%), mixed p99
   from 18.6 to 15.1 ms (-19%), hot account from 131 to 158 cmd/s (+21%);
   those three do not overlap between the two sets of rounds.
3. **Before the first release** (`c315a04` against the optimized code):
   the public modules, the envelope without per-event copies, the wake-up
   between instances in the readers' loop and the packaging changed nothing
   measurable; every gate measure passes.

### How the gate is run

The rule is not to fall behind cqrs-es. The gate runs at the end of each
phase of work, and after any commit that introduces `dyn` or touches the
append path:

```text
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run --release -p cqrs-versus -- 3
cargo bench -p cqrs-benchmarks --bench micro
```

The thresholds are in the table above. To compare two versions of the
code, one run of each is not enough: build both release binaries and run
them alternately (base, head, base, head, base, head) under the same load,
then compare the medians and whether the rounds overlap. The
micro-benchmarks are informative: a regression there needs an explanation
in the commit, but the table decides.

### Noise on this machine

All numbers come from one laptop (Apple M4 Pro, 14 cores) with PostgreSQL
17 in Docker on the same machine, so the benchmark's 32 clients per system,
both libraries and the database share the CPU. What that means for reading
them:

- **Load moves everything.** Load average was 6.7–12.5 during the latest
  run (the "after" rounds started under slightly more load) and 10–13
  during the A/B before it, mostly the benchmark itself. Earlier, with
  other processes running (load 10–22), the same binary moved between 2700
  and 4500 cmd/s on open account, and the measures that failed changed from
  one run to the next with the code unchanged.
- **Ratios move with cqrs-es's noise too.** A ratio can drop because
  cqrs-es had a fast run, not because ours was slow. Treat single-run
  differences under about 20% as noise; trust only A/B rounds that do not
  overlap.
- **Measures on their limit.** The snapshot replay sits exactly on its
  threshold (0.61) in the latest run, as it did in the baseline: a run
  landing slightly above needs a second run before it is called a
  regression. The mixed p99 threshold comes from a ratio (0.63) this
  machine did not reproduce, with any code, until the PostgreSQL
  optimizations.
- **A quiet machine is the only absolute gate.** The thresholds were
  written for one; on a loaded one, use the A/B method.

### Why

- **Writes.** One statement per append; positions from a sequence, so
  appends never wait for each other; locks only where the domain overlaps
  ([Storage § Locks](storage.md#locks-derived-from-the-domain)). Static
  dispatch adds nothing measurable on top of the database round trips.
- **Replays.** `LoadStream::replay` reads only the sequence, type, version
  and payload, and decodes each row as it arrives; events are stored as
  `json` and decoded straight into their type unless an upcaster applies
  ([Storage § Upcasting](storage.md#upcasting)).
- **Correctness of the read model.** cqrs-es updates views inline, after
  the commit, with a separate read-modify-write per view: concurrent
  commands on the same account race and lose updates. Here the projection
  runs in a tracking processor, each event once, in order per aggregate, in
  a transaction with its token.
- **Rebuild and catching up.** One shared reader per processor routes
  events to 8 segments in parallel, and each segment handles what is
  waiting in batches of up to 100, one commit per batch. Measured on the
  same build (`benchmarks/results/versus.md`), batches took the time until
  everything is done from 9287 to 4870 ms, and a rebuild from 6050 to 41420
  events/s.

## Micro-benchmarks

`benchmarks/benches/micro.rs` (divan): single operations, in memory, no
runtime.

```text
cargo bench -p cqrs-benchmarks --bench micro
cargo bench -p cqrs-benchmarks --bench micro -- interceptors
```

| Benchmark | What it measures | Median |
|---|---|---:|
| `unit/command_gateway` | a command through its gateway and its unit of work | 29.4 ns |
| `persistence/command_cycle` | load a 10-event aggregate, decide, commit, in memory | 1.02 µs |
| `interceptors/command_plain` | a query through its gateway | 3.4 ns |
| `interceptors/command_intercepted_4` | the same through four pass-through interceptors | 13.6 ns |

(`gate-log.md`, run of `768ac7d`.) Since commands always run in a unit of
work, `dispatch/*` and `interceptors/command_*` route a query (`Echo`), to
keep measuring the routing alone. `unit/command_gateway` was 14.3 ns before
the command's unit of work: the 15 ns more are its begin, append, commit,
announce and retry loop, against a command of about 1 ms on PostgreSQL.

An interceptor chain compiles to nested calls: one that does nothing costs
nothing measurable; four cost about 3 ns each. What an interceptor does is
what it costs. The event benchmarks (`interceptors/event_plain`,
`event_intercepted`, `event_measured`: the blog's projection, alone,
through a pass-through interceptor, through `Measured`) were last recorded
in `docs/benchmarks/baseline.md` (16, 11.5 and 50 ns); the other benchmarks
(aggregate, loading with and without snapshots, reading a page of the log)
are listed there too.

## Guidelines

- Projections: write in the unit of work's transaction, and update rows in
  place instead of find-clone-save.
- Do not check sequences in projections: the processor already delivers
  each aggregate's events once and in order.
- Give the namespaces that write a read model a batch (`batch = 64` on the
  handler): one commit per batch of waiting events instead of one per
  event. Keep side effects at 1.
- Raise `segments` for read models with many aggregates; it is fixed by the
  first instance that initializes the processor's tokens.
- Snapshot aggregates with long streams: `snapshot(after_events = 100)` on
  the aggregate (`#[cqrs::aggregate(..)]`), and the engine's repository keeps them.
- Keep side effects in their own namespace, so a slow external call never
  holds the read model back.

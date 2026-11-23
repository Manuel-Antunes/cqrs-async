# Performance

## Against cqrs-es

`benchmarks/versus` runs the same bank on this library and on cqrs-es 0.5
with postgres-es 0.5, against the same PostgreSQL: open, deposit, withdraw
and transfer, one aggregate per account, a balance read model, and a
transfer policy crediting the target account. The business rules live in
one shared module; each framework only wraps them, wired the way its docs
recommend.

```text
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run --release -p cqrs-versus -- 3
```

Latest run (Apple M4 Pro, PostgreSQL 17 in Docker on the same machine,
32 clients and 32 connections per system, median of 3 runs, processors with
8 segments and `batch_size: 100`; treat differences under ~20% as noise):

| Scenario | cqrs (ours) | cqrs-es 0.5 | ours vs theirs |
|---|---:|---:|---:|
| open account, cmd/s | 3666 | 2651 | 1.38× |
| mixed load (1k accounts), cmd/s | 3379 | 2150 | 1.57× |
| mixed load, p99 | 21.7 ms | 34.6 ms | 0.63× |
| mixed load: done, read model and transfers included | 5999 ms | 9302 ms | 0.64× |
| hot account (32 writers, one aggregate), cmd/s | 111 | 96 | 1.15× |
| hot account, p99 | 1413 ms | 1929 ms | 0.73× |
| read model rebuild, events/s | 32257 | 1548 | 20.8× |
| read model after the run: events lost | 0 | 258 | |
| read model after the run: money lost | 0 | 1324 | |
| 5000-event aggregate, full replay, p50 / p99 (after the `json` change, A/B) | 4.0 / 7.1 ms | 5.1 / 7.3 ms | 0.79× / 0.97× |
| 5000-event aggregate, snapshot every 100, p50 / p99 | 1.4 / 3.6 ms | 2.8 / 7.5 ms | 0.51× / 0.48× |

Every run is in `benchmarks/results/versus.md`, with the setup and how
each number is measured.

### Why

- **Writes.** One statement per append; positions from a sequence, so
  appends never wait for each other; locks only where the domain overlaps
  ([Storage § Locks](storage.md#locks-derived-from-the-domain)). Static dispatch
  adds nothing measurable on top of the database round trips.
- **Correctness of the read model.** cqrs-es updates views inline, after
  the commit, with a separate read-modify-write per view: concurrent
  commands on the same account race and lose updates. Here the projection
  runs in a tracking processor, each event once, in order per aggregate, in
  a transaction with its token.
- **Rebuild.** A processor reads the log in pages through one shared
  reader and routes events to 8 segments in parallel.

### Serialization

Events are stored as `json` (text, as written) and decoded straight into
the event type unless an upcaster applies. Against the earlier `jsonb`
columns with a `Value` per event (A/B, same machine and load): a full
replay of a 5,000-event aggregate went from 5.40 to 4.02 ms at the median
and from 8.61 to 7.10 ms at p99, against cqrs-es's 5.12 and 7.32 ms in the
same runs. Details in `benchmarks/results/versus.md` and
[Storage § Upcasting](storage.md#upcasting).

### Batches

With `batch_size: 1` (one transaction per event), the time until
everything is done was 1.27–1.44× theirs: their view and policy run inside
the writer's request, ours ran after it, paying a commit per event. With
`batch_size: 100` the processors share one commit among the events waiting
for a segment: the same run takes 4870 ms instead of 9287 ms (0.74× theirs)
and a rebuild goes from 6050 to 41420 events/s. Command throughput in the
mixed load drops from 4716 to 4206 cmd/s, because the processors now keep
pace with the writers on the same database instead of falling behind.

## Micro-benchmarks

`benchmarks/benches/micro.rs` (divan), in memory, per operation:

| | median |
|---|---:|
| command through a derived gateway | 4.0 ns |
| … the same gateway, held by a `Cqrs` unit (`unit.commands().send`) | 4.2 ns (same, within noise) |
| … through one pass-through interceptor | 4.1 ns |
| … through four interceptors | 15.6 ns |
| event handled by the blog's projection | 18.5 ns |
| … through one pass-through event interceptor | 15.5 ns (same, within noise) |
| … through `Measured` (two clock reads, two atomics) | 59 ns |

An interceptor chain compiles to nested calls: one that does nothing costs
nothing; four cost about 3 ns each. What an interceptor does is what it
costs.

```text
cargo bench -p cqrs-benchmarks --bench micro -- interceptors
```

## Guidelines

- Projections: write in the unit of work's transaction, and use in-place
  updates (`update(id, |row| ..)`) instead of find-clone-save.
- Do not check sequences in projections: the processor already delivers
  each aggregate's events once and in order.
- Set `batch_size` (e.g. 100) on processors whose handlers write to a
  database: one commit per batch of waiting events instead of one per
  event.
- Raise `segments` for read models with many aggregates; it is fixed by
  the first instance that initializes the processor's tokens.
- Snapshot aggregates with long streams (`with_snapshots(store, 100)`).
- Keep side effects in separate processors from read models, so a slow
  external call does not hold the read model back.

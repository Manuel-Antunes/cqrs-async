//! Single operations, in memory, no runtime: what one call costs.
//!
//! `cargo bench -p cqrs-benchmarks --bench micro`

use std::{
    hint::black_box,
    sync::atomic::{AtomicU64, Ordering},
};

use blog::domain::{Post, PostId};
use cqrs::Aggregate;
use cqrs::{CommandGateway, Handler, InMemoryEventStore, InMemorySnapshots, ReadLog};
use cqrs_benchmarks::{
    PlainPosts, now,
    ping::{Ping, PingGateway, PingHandler},
    seed_post, seed_posts,
};

fn main() {
    divan::main();
}

/// Sending a trivial command: the cost of routing itself.
mod dispatch {
    use super::*;

    /// Baseline: calling the handler directly.
    #[divan::bench]
    fn direct_call() -> u64 {
        now(PingHandler.handle(Ping(black_box(1)))).unwrap()
    }

    /// Through `#[derive(Gateway)]`.
    #[divan::bench]
    fn derived_gateway(bencher: divan::Bencher) {
        let gateway = PingGateway { ping: PingHandler };
        bencher.bench_local(|| now(gateway.send(Ping(black_box(1)))).unwrap());
    }
}

/// Domain work, no persistence.
mod aggregate {
    use super::*;

    /// Creating a post then `n` more decisions, each appended and applied
    /// (what the repository does around a decision, minus storage).
    #[divan::bench(args = [1, 100, 10_000])]
    fn record_events(bencher: divan::Bencher, n: usize) {
        bencher.bench_local(|| {
            let mut post = Post::initial(&PostId(1));
            let mut events = Vec::with_capacity(n + 1);
            Post::create(PostId(1), "title", &mut events).unwrap();
            post.evolve(events.last().unwrap());
            for i in 0..n {
                post.retitle(if i % 2 == 0 { "a" } else { "b" }, &mut events).unwrap();
                post.evolve(events.last().unwrap());
            }
            post
        });
    }
}

/// The repository and the in-memory store.
mod persistence {
    use super::*;

    /// Loading an aggregate: read its stream and replay every event.
    #[divan::bench(args = [10, 1_000, 100_000])]
    fn load_and_replay(bencher: divan::Bencher, events: usize) {
        let posts = PlainPosts::new();
        now(seed_post(&posts.store, PostId(1), events));
        bencher.bench_local(|| now(posts.find(black_box(PostId(1)))).unwrap());
    }

    /// Loading a 100k-event aggregate with a snapshot every 100 events:
    /// the snapshot plus at most 99 events, instead of all of them.
    #[divan::bench]
    fn load_with_snapshot_100k_events(bencher: divan::Bencher) {
        let posts = PlainPosts::new().with_snapshots(InMemorySnapshots::default(), 100);
        now(seed_post(&posts.store, PostId(1), 100_000));
        // The first load replays everything and keeps the snapshot.
        now(posts.find(PostId(1))).unwrap();
        bencher.bench_local(|| now(posts.find(black_box(PostId(1)))).unwrap());
    }

    /// Saving a brand-new aggregate (one event) to a store that keeps growing.
    #[divan::bench]
    fn add_new_aggregate(bencher: divan::Bencher) {
        let posts = PlainPosts::new();
        let next = AtomicU64::new(1);
        bencher
            .with_inputs(|| PostId(next.fetch_add(1, Ordering::Relaxed)))
            .bench_local_values(|id| {
                let mut events = Vec::new();
                Post::create(id, "title", &mut events).unwrap();
                now(posts.commit(cqrs::New::<Post>(id), events)).unwrap();
            });
    }

    /// Load, decide, commit: one full command cycle on a 10-event aggregate
    /// (each iteration on its own aggregate, so the stream never grows).
    #[divan::bench]
    fn command_cycle(bencher: divan::Bencher) {
        let posts = PlainPosts::new();
        let next = AtomicU64::new(1);
        bencher
            .with_inputs(|| {
                let id = PostId(next.fetch_add(1, Ordering::Relaxed));
                now(seed_post(&posts.store, id, 10));
                id
            })
            .bench_local_values(|id| {
                let post = now(posts.find(id)).unwrap();
                let mut events = Vec::new();
                post.publish(&mut events).unwrap();
                now(posts.commit(post, events)).unwrap();
            });
    }

    /// Reading one page (256 events) of the global log of a 100k-event store.
    #[divan::bench]
    fn read_all_page(bencher: divan::Bencher) {
        let store = InMemoryEventStore::new();
        now(seed_posts(&store, 10_000, 10));
        bencher.bench_local(|| now(store.read_all(black_box(50_000), 256)).unwrap());
    }
}

/// What an interceptor adds around a command and around an event.
mod interceptors {
    use std::{future::Future, sync::Arc, time::SystemTime};

    use blog::{
        application::projections::PostProjection,
        domain::PostEvent,
        infrastructure::{InMemoryNotifier, InMemoryPostSummaryRepository, Measured, ProjectionMetrics},
    };
    use cqrs::{
        DomainEvent, EventChain, EventEnvelope, EventInterceptor, HandlerError, Intercepted, Interceptor, Message,
        Metadata, ProcessingContext, Route, StreamId,
    };

    type Summaries = PostProjection<InMemoryPostSummaryRepository, InMemoryNotifier>;

    fn summaries() -> Summaries {
        PostProjection::new(Arc::default(), Arc::default())
    }

    /// The projection's `summaries` namespace.
    const SUMMARIES: usize = 0;

    fn context(unit: &mut ()) -> ProcessingContext<'_> {
        ProcessingContext::new(unit, false)
    }

    use super::*;

    /// Calls `next`, nothing else.
    struct Pass;

    impl<M: Message> Interceptor<M> for Pass {
        async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
        where
            N: FnOnce(M, Metadata) -> F + Send,
            F: Future<Output = Result<M::Output, M::Error>> + Send,
        {
            next(message, metadata).await
        }
    }

    impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Pass {
        async fn intercept<N: EventChain<Tx>>(
            &self,
            _: &EventEnvelope<E>,
            context: &mut ProcessingContext<'_, Tx>,
            next: N,
        ) -> Result<(), HandlerError> {
            next.proceed(context).await
        }
    }

    /// The gateway alone (baseline).
    #[divan::bench]
    fn command_plain(bencher: divan::Bencher) {
        let gateway = PingGateway { ping: PingHandler };
        bencher.bench_local(|| now(gateway.send(Ping(black_box(1)))).unwrap());
    }

    /// The gateway through one interceptor that only calls `next`.
    #[divan::bench]
    fn command_intercepted(bencher: divan::Bencher) {
        let gateway = Intercepted::new(PingGateway { ping: PingHandler }, Pass);
        bencher.bench_local(|| now(gateway.send(Ping(black_box(1)))).unwrap());
    }

    /// The same, through a chain of four.
    #[divan::bench]
    fn command_intercepted_4(bencher: divan::Bencher) {
        let gateway = Intercepted::new(PingGateway { ping: PingHandler }, (Pass, Pass, Pass, Pass));
        bencher.bench_local(|| now(gateway.send(Ping(black_box(1)))).unwrap());
    }

    /// One post's `Retitled` event, as the blog's projection gets it.
    fn retitled(summaries: &Summaries) -> EventEnvelope<PostEvent> {
        let created = EventEnvelope {
            position: 1,
            stream: StreamId::new("post", "1"),
            sequence: 1,
            event_type: "post.created".into(),
            event_version: 1,
            recorded_at: SystemTime::now(),
            metadata: Metadata::new(),
            payload: PostEvent::Created {
                id: PostId(1),
                title: "a".into(),
            },
        };
        now(summaries.handle(SUMMARIES, &created, &mut context(&mut ()))).unwrap();
        let payload = PostEvent::Retitled { title: "b".into() };
        EventEnvelope {
            position: 2,
            sequence: 2,
            event_type: payload.event_type().into(),
            payload,
            ..created
        }
    }

    /// The blog's projection applying an event (redelivered, so skipped
    /// after its first time: the handler's own cost stays tiny).
    #[divan::bench]
    fn event_plain(bencher: divan::Bencher) {
        let summaries = summaries();
        let event = retitled(&summaries);
        bencher.bench_local(|| now(summaries.handle(SUMMARIES, black_box(&event), &mut context(&mut ()))).unwrap());
    }

    /// The same, through an interceptor that only calls `next`.
    #[divan::bench]
    fn event_intercepted(bencher: divan::Bencher) {
        let summaries = Intercepted::new(summaries(), Pass);
        let event = retitled(summaries.inner());
        bencher.bench_local(|| now(summaries.handle(SUMMARIES, black_box(&event), &mut context(&mut ()))).unwrap());
    }

    /// The same, through the blog's `Measured` (a clock read, two atomics).
    #[divan::bench]
    fn event_measured(bencher: divan::Bencher) {
        let summaries = Intercepted::new(summaries(), Measured(Arc::new(ProjectionMetrics::default())));
        let event = retitled(summaries.inner());
        bencher.bench_local(|| now(summaries.handle(SUMMARIES, black_box(&event), &mut context(&mut ()))).unwrap());
    }
}

/// The `Cqrs` unit adds no step: a command sent through `unit.commands()`
/// is the gateway's call.
mod unit {
    use cqrs::Cqrs;
    use cqrs_benchmarks::ping::{NoStore, PingCommandHandler, PingCommands};

    use super::*;

    /// A gateway of commands and its unit of work (baseline).
    #[divan::bench]
    fn command_gateway(bencher: divan::Bencher) {
        let gateway = PingCommands {
            ping: PingCommandHandler,
            events: NoStore,
        };
        bencher.bench_local(|| now(gateway.send(Ping(black_box(1)))).unwrap());
    }

    /// The same gateway, built from the unit's publisher and held by it.
    #[divan::bench]
    fn command_through_unit(bencher: divan::Bencher) {
        let unit = Cqrs::from_publisher(NoStore)
            .commands(|events| PingCommands {
                ping: PingCommandHandler,
                events,
            })
            .queries(())
            .build();
        bencher.bench_local(|| now(unit.commands().send(Ping(black_box(1)))).unwrap());
    }
}

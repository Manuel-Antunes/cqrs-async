//! Throughput and latency under concurrency, on a multi-threaded Tokio
//! runtime.
//!
//! ```text
//! cargo run -p cqrs-benchmarks --release --bin load -- <label> [--quick]
//! ```
//!
//! Prints a table and writes it to `benchmarks/results/<label>.md`.

use std::{
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use blog::application::queries::FindPost;
use blog::{
    application::projections::PostSummaryProjection,
    application::{
        BlogError,
        commands::{CreatePost, RetitlePost},
    },
    domain::{Post, PostId},
    infrastructure::InMemoryPostSummaryRepository,
    presentation::{BlogApi, graphql, http},
};
use cqrs::{CommandGateway, QuerySubscriptions};
use cqrs_benchmarks::{
    live::{Busy, Live, Noop, settle, start_blog},
    seed_posts,
};
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::{Request, body::Bytes};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};

/// One measured number.
struct Row {
    scenario: String,
    metric: &'static str,
    value: f64,
    unit: &'static str,
}

#[derive(Default)]
struct Report {
    rows: Vec<Row>,
}

impl Report {
    fn add(&mut self, scenario: impl Into<String>, metric: &'static str, value: f64, unit: &'static str) {
        let scenario = scenario.into();
        println!("{scenario:<58} {metric:<14} {value:>14.1} {unit}");
        self.rows.push(Row {
            scenario,
            metric,
            value,
            unit,
        });
    }

    fn markdown(&self, label: &str) -> String {
        let mut out = String::new();
        let rustc = std::process::Command::new("rustc")
            .arg("-V")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default();
        let cpu = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default();
        let _ = writeln!(out, "# Load benchmarks: `{label}`\n");
        // Load average: other processes skew results, so record how busy the machine was.
        let load = std::process::Command::new("sysctl")
            .args(["-n", "vm.loadavg"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "{cpu}, {} threads, {rustc}, release profile, load average at the end {load}. Client and server share the machine in the HTTP scenarios.\n",
            std::thread::available_parallelism().map_or(0, |n| n.get()),
        );
        let _ = writeln!(out, "| Scenario | Metric | Value | Unit |\n|---|---|---:|---|");
        for row in &self.rows {
            let _ = writeln!(
                out,
                "| {} | {} | {:.1} | {} |",
                row.scenario, row.metric, row.value, row.unit
            );
        }
        out
    }
}

/// Latency percentiles of a sample, in microseconds.
fn percentiles(mut samples: Vec<Duration>) -> (f64, f64, f64) {
    samples.sort_unstable();
    let at = |p: f64| {
        let index = ((samples.len() as f64 * p) as usize).min(samples.len() - 1);
        samples[index].as_secs_f64() * 1e6
    };
    (at(0.50), at(0.99), at(0.999))
}

struct Scale {
    commands: u64,
    contention_rounds: u64,
    fan_out_deliveries: u64,
    processor_events: u64,
    catch_up_events: u64,
    http_duration: Duration,
    live_query_subscribers: u64,
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let label = args.next().unwrap_or_else(|| "run".to_owned());
    let quick = args.any(|arg| arg == "--quick");
    let scale = if quick {
        Scale {
            commands: 20_000,
            contention_rounds: 200,
            fan_out_deliveries: 1_000_000,
            processor_events: 20_000,
            catch_up_events: 100_000,
            http_duration: Duration::from_secs(2),
            live_query_subscribers: 100,
        }
    } else {
        Scale {
            commands: 200_000,
            contention_rounds: 2_000,
            fan_out_deliveries: 10_000_000,
            processor_events: 200_000,
            catch_up_events: 1_000_000,
            http_duration: Duration::from_secs(5),
            live_query_subscribers: 1_000,
        }
    };

    let mut report = Report::default();
    commands(&mut report, &scale).await;
    contention(&mut report, &scale).await;
    fan_out(&mut report, &scale).await;
    live_queries(&mut report, &scale).await;
    processors(&mut report, &scale).await;
    segments(&mut report, &scale).await;
    catch_up(&mut report, &scale).await;
    over_http(&mut report, &scale).await;
    postgres(&mut report, &scale).await;

    let path = format!("{}/results/{label}.md", env!("CARGO_MANIFEST_DIR"));
    std::fs::write(&path, report.markdown(&label)).expect("writing the report");
    println!("\nwritten to {path}");
}

/// CreatePost through the gateway of the full application (broadcast + two
/// tracking processors running), each command on its own aggregate.
async fn commands(report: &mut Report, scale: &Scale) {
    for writers in [1_u64, 4, 14, 64] {
        let running = start_blog();
        let per_writer = scale.commands / writers;
        let started = Instant::now();
        let tasks: Vec<_> = (0..writers)
            .map(|_| {
                let app = Arc::clone(&running.app);
                tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(per_writer as usize);
                    for _ in 0..per_writer {
                        let sent = Instant::now();
                        app.commands()
                            .send(CreatePost {
                                title: "benchmark".to_owned(),
                            })
                            .await
                            .unwrap();
                        latencies.push(sent.elapsed());
                    }
                    latencies
                })
            })
            .collect();
        let mut latencies = Vec::new();
        for task in tasks {
            latencies.extend(task.await.unwrap());
        }
        let committed = started.elapsed();
        settle(&running).await;
        let projected = started.elapsed();

        let total = (per_writer * writers) as f64;
        let name = format!("create post, {writers} concurrent writers");
        let (p50, p99, _) = percentiles(latencies);
        report.add(&name, "throughput", total / committed.as_secs_f64(), "cmd/s");
        report.add(&name, "latency p50", p50, "µs");
        report.add(&name, "latency p99", p99, "µs");
        report.add(&name, "projected", total / projected.as_secs_f64(), "cmd/s");
        running.stop().await;
    }
}

/// Writers racing on one aggregate: optimistic concurrency, retried.
async fn contention(report: &mut Report, scale: &Scale) {
    let writers = 8_u64;
    let running = start_blog();
    let id = running
        .app
        .commands()
        .send(CreatePost {
            title: "contended".to_owned(),
        })
        .await
        .unwrap();
    let conflicts = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|writer| {
            let app = Arc::clone(&running.app);
            let conflicts = Arc::clone(&conflicts);
            let rounds = scale.contention_rounds;
            tokio::spawn(async move {
                for round in 0..rounds {
                    loop {
                        let command = RetitlePost {
                            id,
                            title: format!("w{writer} r{round}"),
                        };
                        match app.commands().send(command).await {
                            Ok(()) => break,
                            Err(BlogError::Conflict) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                tokio::task::yield_now().await;
                            }
                            Err(error) => panic!("{error}"),
                        }
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let elapsed = started.elapsed();
    let done = (writers * scale.contention_rounds) as f64;
    let name = format!("retitle one post, {writers} racing writers");
    report.add(&name, "throughput", done / elapsed.as_secs_f64(), "cmd/s");
    report.add(
        &name,
        "conflicts",
        conflicts.load(Ordering::Relaxed) as f64 / done * 100.0,
        "% of cmds",
    );
    running.stop().await;
}

/// N subscribers following the live feed while events are committed.
async fn fan_out(report: &mut Report, scale: &Scale) {
    for subscribers in [1_u64, 100, 10_000] {
        let events = (scale.fan_out_deliveries / subscribers).min(100_000);
        let live = Live::new();
        let tasks: Vec<_> = (0..subscribers)
            .map(|_| {
                let mut stream = Box::pin(live.subscribe(0));
                tokio::spawn(async move {
                    let mut seen = 0;
                    while seen < events {
                        stream.next().await.unwrap().unwrap();
                        seen += 1;
                    }
                })
            })
            .collect();
        tokio::task::yield_now().await;

        let started = Instant::now();
        for n in 1..=events {
            let mut events = Vec::new();
            Post::create(PostId(n), "fan-out", &mut events).unwrap();
            live.commit(cqrs::New::<Post>(PostId(n)), events).await.unwrap();
        }
        let committed = started.elapsed();
        for task in tasks {
            task.await.unwrap();
        }
        let delivered = started.elapsed();

        let name = format!("fan-out, {subscribers} subscribers x {events} events");
        report.add(&name, "commit", events as f64 / committed.as_secs_f64(), "events/s");
        report.add(
            &name,
            "delivery",
            (events * subscribers) as f64 / delivered.as_secs_f64(),
            "deliveries/s",
        );
    }
}

/// Many clients each subscribed to one post (`post(id)` in GraphQL) while
/// writers change every post: how long until every client shows the final
/// state. Exercises the whole chain: commit, broadcast, projection,
/// re-query after the projection, dedupe.
async fn live_queries(report: &mut Report, scale: &Scale) {
    let subscribers = scale.live_query_subscribers;
    let rounds = 10_u64;
    let running = start_blog();
    let mut ids = Vec::new();
    for _ in 0..subscribers {
        let id = running
            .app
            .commands()
            .send(CreatePost { title: "v0".to_owned() })
            .await
            .unwrap();
        ids.push(id);
    }
    settle(&running).await;

    let watchers: Vec<_> = ids
        .iter()
        .map(|&id| {
            let mut updates = Box::pin(running.app.queries().subscribe(FindPost { id }));
            tokio::spawn(async move {
                let final_title = format!("v{rounds}");
                let mut seen = 0_u64;
                while let Some(post) = updates.next().await {
                    seen += 1;
                    if post.unwrap().is_some_and(|post| post.title == final_title) {
                        return seen;
                    }
                }
                panic!("subscription ended");
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await; // let every subscription start

    let started = Instant::now();
    let writers: Vec<_> = ids
        .chunks(ids.len().div_ceil(14))
        .map(|chunk| {
            let app = Arc::clone(&running.app);
            let chunk = chunk.to_vec();
            tokio::spawn(async move {
                for round in 1..=rounds {
                    for &id in &chunk {
                        app.commands()
                            .send(RetitlePost {
                                id,
                                title: format!("v{round}"),
                            })
                            .await
                            .unwrap();
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    let committed = started.elapsed();
    let mut pushed = 0;
    for watcher in watchers {
        pushed += watcher.await.unwrap();
    }
    let converged = started.elapsed();

    let commands = (subscribers * rounds) as f64;
    let name = format!("live queries, {subscribers} subscribers x {rounds} changes each");
    report.add(&name, "commands", commands / committed.as_secs_f64(), "cmd/s");
    report.add(&name, "all converged", converged.as_secs_f64() * 1e3, "ms");
    report.add(&name, "pushes", pushed as f64 / commands * 100.0, "% of changes");
    running.stop().await;
}

/// A tracking processor catching up on an existing log.
async fn processors(report: &mut Report, scale: &Scale) {
    let events = scale.processor_events;
    for (name, projection) in [("no-op handler", false), ("post summaries projection", true)] {
        let live = Live::new();
        seed_posts(&live.store, events, 1).await;
        let started = Instant::now();
        let handle = if projection {
            live.track(
                "bench",
                PostSummaryProjection::new(Arc::new(InMemoryPostSummaryRepository::default())),
            )
        } else {
            live.track("bench", Noop)
        };
        handle.caught_up(live.last_position().await).await;
        let elapsed = started.elapsed();
        report.add(
            format!("tracking processor, {name}, {events} events"),
            "throughput",
            events as f64 / elapsed.as_secs_f64(),
            "events/s",
        );
        handle.stop().await;
    }
}

/// A processor with a CPU-bound handler over 1, 4 and 14 segments: how far
/// splitting by aggregate scales the read side.
async fn segments(report: &mut Report, scale: &Scale) {
    let events = scale.processor_events / 2;
    let live = Live::new();
    seed_posts(&live.store, events, 1).await; // one event per aggregate: spread evenly
    let last = live.last_position().await;
    for segments in [1_u32, 4, 14] {
        let options = cqrs::TrackingOptions {
            segments,
            ..cqrs::TrackingOptions::default()
        };
        let started = Instant::now();
        let handle = live
            .events()
            .processor(&format!("busy-{segments}"), Busy(2_000))
            .with_options(options)
            .spawn();
        handle.caught_up(last).await;
        let elapsed = started.elapsed();
        report.add(
            format!("tracking processor, CPU-bound handler, {segments} segments, {events} events"),
            "throughput",
            events as f64 / elapsed.as_secs_f64(),
            "events/s",
        );
        handle.stop().await;
    }
}

/// One subscription reading a large log from the start.
async fn catch_up(report: &mut Report, scale: &Scale) {
    let events = scale.catch_up_events;
    let live = Live::new();
    seed_posts(&live.store, events / 10, 10).await;
    let started = Instant::now();
    let mut stream = Box::pin(live.subscribe(0));
    for _ in 0..events {
        stream.next().await.unwrap().unwrap();
    }
    let elapsed = started.elapsed();
    report.add(
        format!("catch-up subscription, {events} events"),
        "throughput",
        events as f64 / elapsed.as_secs_f64(),
        "events/s",
    );
}

/// The GraphQL API over real HTTP, keep-alive connections.
async fn over_http(report: &mut Report, scale: &Scale) {
    let running = start_blog();
    let id = running
        .app
        .commands()
        .send(CreatePost {
            title: "read me".to_owned(),
        })
        .await
        .unwrap();
    settle(&running).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let schema = graphql::schema(Arc::clone(&running.app));
    let server = tokio::spawn(async move {
        axum::serve(listener, http::router(schema)).await.unwrap();
    });

    let mutation = r#"{"query":"mutation { createPost(title: \"over http\") }"}"#.to_owned();
    let query = format!(r#"{{"query":"{{ post(id: \"{id}\") {{ title published }} }}"}}"#);
    for (name, body) in [("mutation createPost", mutation), ("query post(id)", query)] {
        let connections = 64;
        let url = format!("http://{address}{}", http::GRAPHQL_PATH);
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(connections)
            .build_http::<Full<Bytes>>();
        let deadline = Instant::now() + scale.http_duration;
        let started = Instant::now();
        let tasks: Vec<_> = (0..connections)
            .map(|_| {
                let client = client.clone();
                let url = url.clone();
                let body = Bytes::from(body.clone());
                tokio::spawn(async move {
                    let mut latencies = Vec::new();
                    let mut errors = 0_u64;
                    while Instant::now() < deadline {
                        let request = Request::post(&url)
                            .header("content-type", "application/json")
                            .body(Full::new(body.clone()))
                            .unwrap();
                        let sent = Instant::now();
                        let response = client.request(request).await.unwrap();
                        let ok = response.status().is_success();
                        let bytes = response.into_body().collect().await.unwrap().to_bytes();
                        latencies.push(sent.elapsed());
                        if !ok || bytes.windows(8).any(|w| w == b"\"errors\"") {
                            errors += 1;
                        }
                    }
                    (latencies, errors)
                })
            })
            .collect();
        let mut latencies = Vec::new();
        let mut errors = 0;
        for task in tasks {
            let (l, e) = task.await.unwrap();
            latencies.extend(l);
            errors += e;
        }
        let elapsed = started.elapsed();
        assert_eq!(errors, 0, "{name}: {errors} failed requests");
        let requests = latencies.len() as f64;
        let (p50, p99, _) = percentiles(latencies);
        let scenario = format!("HTTP GraphQL {name}, {connections} connections");
        report.add(&scenario, "throughput", requests / elapsed.as_secs_f64(), "req/s");
        report.add(&scenario, "latency p50", p50, "µs");
        report.add(&scenario, "latency p99", p99, "µs");
    }

    server.abort();
    running.stop().await;
}

/// Appends to PostgreSQL with N concurrent writers (one event per append,
/// each writer on its own streams), if `CQRS_POSTGRES_URL` is set. Appends
/// are serialized by the head row, so this is the price of gapless order.
async fn postgres(report: &mut Report, scale: &Scale) {
    use cqrs::{AppendEvents, ExpectedVersion, Metadata, ReadLog, StreamId};

    #[derive(Clone, serde::Serialize, serde::Deserialize, cqrs::DomainEvent)]
    struct Ticked(u64);

    struct Clock;
    impl cqrs::Aggregate for Clock {
        const TYPE: &'static str = "clock";
        type Id = u64;
        type Event = Ticked;
        fn initial(_: &u64) -> Self {
            Clock
        }
        fn id(&self) -> &u64 {
            &0
        }
        fn evolve(&mut self, _: &Ticked) {}
    }

    let Ok(url) = std::env::var("CQRS_POSTGRES_URL") else {
        println!("(PostgreSQL scenarios skipped: CQRS_POSTGRES_URL not set)");
        return;
    };
    let total = (scale.commands / 20).max(2_000);
    for writers in [1_u64, 4, 16, 64] {
        let schema = format!("bench_{}_{writers}", std::process::id());
        let admin = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        )))
        .execute(&admin)
        .await
        .ok();
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA IF NOT EXISTS {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(64)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .unwrap();
        cqrs_postgres::migrate(&pool).await.unwrap();
        let store = cqrs_postgres::PostgresEventStore::<Ticked>::new(pool, "ticks");

        let per_writer = total / writers;
        let started = Instant::now();
        let tasks: Vec<_> = (0..writers)
            .map(|writer| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(per_writer as usize);
                    for n in 0..per_writer {
                        let stream = StreamId::of::<Clock>(&(writer * 1_000_000 + n));
                        let sent = Instant::now();
                        store
                            .append(
                                &mut (),
                                &stream,
                                ExpectedVersion::NoStream,
                                vec![Ticked(n)],
                                &Metadata::new(),
                            )
                            .await
                            .unwrap();
                        latencies.push(sent.elapsed());
                    }
                    latencies
                })
            })
            .collect();
        let mut latencies = Vec::new();
        for task in tasks {
            latencies.extend(task.await.unwrap());
        }
        let elapsed = started.elapsed();
        let appends = latencies.len() as f64;
        let (p50, p99, _) = percentiles(latencies);
        let name = format!("PostgreSQL append, {writers} concurrent writers");
        report.add(&name, "throughput", appends / elapsed.as_secs_f64(), "appends/s");
        report.add(&name, "latency p50", p50, "µs");
        report.add(&name, "latency p99", p99, "µs");
        assert_eq!(
            store.last_position().await.unwrap(),
            (per_writer * writers),
            "gapless under load"
        );
    }
}

//! The same bank on this crate and on cqrs-es/postgres-es, against the same
//! PostgreSQL, measured side by side. Each run builds every system on a
//! fresh schema; the report takes the median of the runs.
//!
//! ```text
//! CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs \
//!     cargo run --release -p cqrs-versus -- [runs]
//! ```

mod bank;
mod ours;
mod system;
mod theirs;

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bank::{BankCommand, BankError};
use system::System;

/// Concurrent clients, and connections per system.
const WORKERS: usize = 32;
const ACCOUNTS: usize = 5_000;
/// Accounts the mixed workload and its transfers use.
const ACTIVE: usize = 1_000;
const MIXED: usize = 20_000;
const HOT: usize = 2_000;
const HISTORY: usize = 5_000;
const ON_LONG_HISTORY: usize = 200;

/// One measured number, by scenario and system.
#[derive(Default)]
struct Report {
    /// (scenario, metric, unit) -> system -> values of each run.
    rows: BTreeMap<(usize, String, &'static str), BTreeMap<&'static str, Vec<f64>>>,
    order: usize,
}

impl Report {
    fn add(&mut self, metric: &str, unit: &'static str, system: &'static str, value: f64) {
        let order = self.rows.keys().find(|(_, name, _)| name == metric).map(|(order, ..)| *order).unwrap_or_else(|| {
            self.order += 1;
            self.order
        });
        self.rows.entry((order, metric.to_owned(), unit)).or_default().entry(system).or_default().push(value);
    }
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

/// What a workload measured.
struct Stats {
    wall: Duration,
    latencies: Vec<Duration>,
    conflicts: u64,
    refused: u64,
}

impl Stats {
    fn per_second(&self) -> f64 {
        self.latencies.len() as f64 / self.wall.as_secs_f64()
    }

    fn percentile(&self, p: f64) -> f64 {
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        sorted[((sorted.len() - 1) as f64 * p) as usize].as_secs_f64() * 1e3
    }
}

/// Runs `commands` with `workers` concurrent clients, each retrying a lost
/// race until it is decided (the same policy for both systems).
async fn drive<S: System>(system: &Arc<S>, workers: usize, commands: Vec<(String, BankCommand)>) -> Stats {
    let commands = Arc::new(commands);
    let next = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let refused = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let tasks: Vec<_> = (0..workers)
        .map(|_| {
            let (system, commands, next) = (Arc::clone(system), Arc::clone(&commands), Arc::clone(&next));
            let (conflicts, refused) = (Arc::clone(&conflicts), Arc::clone(&refused));
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                while let Some((account, command)) = commands.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let sent = Instant::now();
                    loop {
                        match system.execute(account, command.clone()).await {
                            Err(BankError::Conflict) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(BankError::Store(error)) => panic!("store error: {error}"),
                            Err(_) => {
                                refused.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Ok(()) => break,
                        }
                    }
                    latencies.push(sent.elapsed());
                }
                latencies
            })
        })
        .collect();
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.expect("a worker"));
    }
    Stats { wall: start.elapsed(), latencies, conflicts: conflicts.load(Ordering::Relaxed), refused: refused.load(Ordering::Relaxed) }
}

/// xorshift: the same workload for both systems.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn account(n: usize) -> String {
    format!("acc-{n}")
}

fn mixed_workload() -> Vec<(String, BankCommand)> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..MIXED)
        .map(|_| {
            let from = rng.below(ACTIVE as u64) as usize;
            let command = match rng.below(100) {
                0..55 => BankCommand::Deposit { amount: 1 + rng.below(100) as i64 },
                55..80 => BankCommand::Withdraw { amount: 1 + rng.below(200) as i64 },
                _ => BankCommand::Transfer {
                    to: account((from + 1 + rng.below(ACTIVE as u64 - 1) as usize) % ACTIVE),
                    amount: 1 + rng.below(100) as i64,
                },
            };
            (account(from), command)
        })
        .collect()
}

/// How far the read model is from the log: accounts whose row is missing or
/// wrong, and the money and events it lost in total.
fn drift(views: &bank::Balances, truth: &bank::Balances) -> (usize, i64, i64) {
    let mut wrong = 0;
    let (mut money, mut events) = (0, 0);
    for (account, (balance, count)) in truth {
        let (seen, seen_count) = views.get(account).copied().unwrap_or_default();
        if (seen, seen_count) != (*balance, *count) {
            wrong += 1;
            money += (balance - seen).abs();
            events += count - seen_count;
        }
    }
    (wrong, money, events)
}

/// The bank workload on one system: opens, a mixed load with transfers, a
/// hot account, then how consistent the read model is, and its rebuild.
async fn bank_scenarios<S: System>(name: &'static str, system: S, report: &mut Report) {
    let system = Arc::new(system);
    let opens = (0..ACCOUNTS).map(|n| (account(n), BankCommand::Open)).collect();
    let stats = drive(&system, WORKERS, opens).await;
    report.add("open account (new aggregate)", "cmd/s", name, stats.per_second());
    report.add("open account: p99", "ms", name, stats.percentile(0.99));

    let funding = (0..ACTIVE).map(|n| (account(n), BankCommand::Deposit { amount: 10_000 })).collect();
    drive(&system, WORKERS, funding).await;
    system.settle().await;

    let started = Instant::now();
    let stats = drive(&system, WORKERS, mixed_workload()).await;
    system.settle().await;
    let settled = started.elapsed();
    report.add("mixed (deposit/withdraw/transfer), 1k accounts", "cmd/s", name, stats.per_second());
    report.add("mixed: p50", "ms", name, stats.percentile(0.50));
    report.add("mixed: p99", "ms", name, stats.percentile(0.99));
    report.add("mixed: conflicts retried", "per 1k cmds", name, stats.conflicts as f64 * 1e3 / MIXED as f64);
    report.add("mixed: refused by the rules", "cmds", name, stats.refused as f64);
    report.add("mixed: done, read model and transfers included", "ms", name, settled.as_secs_f64() * 1e3);

    let hot = (0..HOT).map(|_| (account(0), BankCommand::Deposit { amount: 1 })).collect();
    let started = Instant::now();
    let stats = drive(&system, WORKERS, hot).await;
    system.settle().await;
    report.add("hot account (32 writers, one account)", "cmd/s", name, stats.per_second());
    report.add("hot account: p99", "ms", name, stats.percentile(0.99));
    report.add("hot account: conflicts retried", "per cmd", name, stats.conflicts as f64 / HOT as f64);
    report.add("hot account: done, read model included", "ms", name, started.elapsed().as_secs_f64() * 1e3);

    let truth = system.truth().await;
    let (wrong, money, events) = drift(&system.views().await, &truth);
    report.add("read model: accounts wrong", "accounts", name, wrong as f64);
    report.add("read model: money lost", "units", name, money as f64);
    report.add("read model: events lost", "events", name, events as f64);

    let started = Instant::now();
    let replayed = system.rebuild().await;
    report.add("read model rebuild", "events/s", name, replayed as f64 / started.elapsed().as_secs_f64());
    let (wrong, ..) = drift(&system.views().await, &system.truth().await);
    report.add("read model after rebuild: accounts wrong", "accounts", name, wrong as f64);
    system.close().await;
}

/// Commands on an account with a long history, one at a time.
async fn long_history<S: System>(label: &str, name: &'static str, system: S, report: &mut Report) {
    let system = Arc::new(system);
    system.execute("long", BankCommand::Open).await.expect("opening");
    system.seed("long", HISTORY).await;
    system.settle().await;
    let deposits = (0..ON_LONG_HISTORY).map(|_| ("long".to_owned(), BankCommand::Deposit { amount: 1 })).collect();
    let stats = drive(&system, 1, deposits).await;
    report.add(&format!("{HISTORY}-event account, {label}: p50"), "ms", name, stats.percentile(0.50));
    report.add(&format!("{HISTORY}-event account, {label}: p99"), "ms", name, stats.percentile(0.99));
    system.close().await;
}

/// Fresh schemas on one database, for both sqlx versions.
struct Database {
    url: String,
    admin: sqlx::PgPool,
    schemas: Vec<String>,
}

impl Database {
    async fn schema(&mut self) -> String {
        let schema = format!("versus_{}_{}", std::process::id(), self.schemas.len());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}"))).execute(&self.admin).await.expect("schema");
        self.schemas.push(schema.clone());
        schema
    }

    async fn ours(&mut self) -> sqlx::PgPool {
        let schema = self.schema().await;
        let options: sqlx::postgres::PgConnectOptions = self.url.parse().expect("a postgres url");
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(WORKERS as u32)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .expect("connecting")
    }

    async fn theirs(&mut self) -> sqlx08::PgPool {
        let schema = self.schema().await;
        let options: sqlx08::postgres::PgConnectOptions = self.url.parse().expect("a postgres url");
        sqlx08::postgres::PgPoolOptions::new()
            .max_connections(WORKERS as u32)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .expect("connecting")
    }

    async fn drop_all(&self) {
        for schema in &self.schemas {
            sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE"))).execute(&self.admin).await.ok();
        }
    }
}

const OURS: &str = "cqrs (ours)";
const THEIRS: &str = "cqrs-es 0.5";

async fn timed<F: Future<Output = ()>>(what: &str, future: F) {
    let started = Instant::now();
    future.await;
    eprintln!("  {what}: {:.1}s", started.elapsed().as_secs_f64());
}

#[tokio::main]
async fn main() {
    let url = std::env::var("CQRS_POSTGRES_URL").expect("set CQRS_POSTGRES_URL");
    let runs: usize = std::env::args().nth(1).map_or(3, |runs| runs.parse().expect("a number of runs"));
    let admin = sqlx::PgPool::connect(&url).await.expect("connecting");
    let mut database = Database { url, admin, schemas: Vec::new() };
    let mut report = Report::default();

    for run in 0..runs {
        eprintln!("run {}/{runs}", run + 1);
        // Alternate who goes first, so neither always gets a warmer database.
        for first in [run % 2 == 0, run % 2 == 1] {
            if first {
                let pool = database.ours().await;
                timed(OURS, bank_scenarios(OURS, ours::Ours::new(pool).await, &mut report)).await;
            } else {
                let pool = database.theirs().await;
                timed(THEIRS, bank_scenarios(THEIRS, theirs::Theirs::new(pool, None).await, &mut report)).await;
            }
        }
        let pool = database.ours().await;
        long_history("full replay", OURS, ours::Ours::new(pool).await, &mut report).await;
        let pool = database.theirs().await;
        long_history("full replay", THEIRS, theirs::Theirs::new(pool, None).await, &mut report).await;
        let pool = database.ours().await;
        long_history("snapshot every 100", OURS, ours::Ours::with_snapshots(pool, 100).await, &mut report).await;
        let pool = database.theirs().await;
        long_history("snapshot every 100", THEIRS, theirs::Theirs::new(pool, Some(100)).await, &mut report).await;
        database.drop_all().await;
        database.schemas.clear();
    }

    let mut table = format!(
        "| Scenario | Unit | {OURS} | {THEIRS} | ours vs theirs |\n|---|---|---:|---:|---:|\n"
    );
    for ((_, metric, unit), systems) in &report.rows {
        let (ours, theirs) = (median(&systems[OURS]), median(&systems[THEIRS]));
        let ratio = if theirs == 0.0 || ours == 0.0 { "—".to_owned() } else { format!("{:.2}×", ours / theirs) };
        let _ = writeln!(table, "| {metric} | {unit} | {} | {} | {ratio} |", number(ours), number(theirs));
    }
    println!("{table}");
    println!("median of {runs} runs; {WORKERS} clients and {WORKERS} connections per system");
}

fn number(value: f64) -> String {
    if value >= 100.0 { format!("{value:.0}") } else { format!("{value:.2}") }
}

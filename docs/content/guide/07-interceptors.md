---
title: "7. Interceptors"
sidebar_position: 17
---

Logging, metrics, auditing and authorization apply to every message, not
to one use case, so they stay out of the handlers. Two kinds of
interceptor, as in Axon:

- **dispatch interceptors** (`Interceptor<M>`) around a gateway: every
  command or query, with its metadata, before and after its handler;
- **handler interceptors** (`EventInterceptor<E, Tx>`) around an event
  handler or a projection, inside its unit of work, with its processing
  context.

Both wrap with `Intercepted::new(inner, interceptors)`, and a tuple chains
several, the first outermost. The blog keeps its two in
`src/infrastructure/observability.rs`.

## Auditing every command

```rust
// src/infrastructure/observability.rs
use cqrs::{
    Interceptor, Message,
    event::{DomainEvent, EventEnvelope, Metadata},
    processing::{EventChain, EventInterceptor, HandlerError, ProcessingContext},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    pub command: &'static str,
    pub correlation_id: Option<String>,
    pub succeeded: bool,
}

#[derive(Debug, Default)]
pub struct AuditLog(Mutex<Vec<AuditEntry>>);

/// Records every command the gateway handles, after it is handled.
#[derive(Clone)]
pub struct Audit(pub Arc<AuditLog>);

impl<M: Message> Interceptor<M> for Audit {
    async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send,
    {
        let correlation_id = metadata.get("correlation_id").cloned();
        let result = next(message, metadata).await;
        let command = std::any::type_name::<M>().rsplit("::").next().unwrap_or_default();
        let entry = AuditEntry { command, correlation_id, succeeded: result.is_ok() };
        self.0.0.lock().expect("audit log poisoned").push(entry);
        result
    }
}
```

One impl, generic over the message, covers every command the gateway
routes. The unit wraps its whole command gateway with `.intercept`:

```rust
let audit = Arc::new(AuditLog::default());
let app = Cqrs::new(Events::new(store.clone()))
    .commands(|events| PostCommandGateway::new(posts_over(&store), events))
    .intercept(Audit(Arc::clone(&audit)))     // Intercepted<PostCommandGateway<..>, Audit>
    // ...
    .build();

let mut metadata = Metadata::new();
metadata.insert("correlation_id".to_owned(), "req-1".to_owned());
app.commands().send_with(CreatePost { title: "Audited".into() }, metadata).await?;
assert_eq!(audit.entries()[0].correlation_id.as_deref(), Some("req-1"));
```

`Intercepted` is a gateway: it routes every command the inner gateway
handles, so it is still `impl PostCommands`, and nothing downstream
changes. Queries are intercepted the same way, by handing
`Intercepted::new(PostQueryGateway::new(..), interceptor)` to `.queries(..)`.

### Refusing a command

An interceptor refuses by returning an error without calling `next`. To
return a typed error, it requires the message's error to accept it:

```rust
use cqrs::{Interceptor, command::Command, event::Metadata};

pub struct Unauthorized;

impl From<Unauthorized> for BlogError {
    fn from(_: Unauthorized) -> Self { BlogError::Store("unauthorized".into()) }   // or a dedicated variant
}

pub struct RequireUser;

impl<C: Command> Interceptor<C> for RequireUser
where
    C::Error: From<Unauthorized>,
{
    async fn intercept<N, F>(&self, message: C, metadata: Metadata, next: N) -> Result<C::Output, C::Error>
    where
        N: FnOnce(C, Metadata) -> F + Send,
        F: Future<Output = Result<C::Output, C::Error>> + Send,
    {
        if !metadata.contains_key("user") {
            return Err(Unauthorized.into());
        }
        next(message, metadata).await
    }
}

// in the builder: a tuple chains them, the first outermost
    .intercept((Audit(audit), RequireUser))
```

`Audit` is outermost, so refused commands are audited too.

## Measuring the projection

```rust
// src/infrastructure/observability.rs
#[derive(Debug, Default)]
pub struct ProjectionMetrics {
    handled: AtomicU64,
    busy_us: AtomicU64,
}

/// Counts and times every event handled, and traces the ones that fail.
#[derive(Clone)]
pub struct Measured(pub Arc<ProjectionMetrics>);

impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Measured {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        let started = Instant::now();
        let result = next.proceed(context).await;
        let busy = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.0.busy_us.fetch_add(busy, Ordering::Relaxed);
        match &result {
            Ok(()) => {
                self.0.handled.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => tracing::warn!(position = event.position, %error, "projection failed"),
        }
        result
    }
}
```

```rust
let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    // in the builder:
    .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
```

- `next` is the rest of the chain (more interceptors, then the handler).
  `next.proceed(context)` runs it in the same unit of work; not calling it
  skips the event, which then counts as handled.
- Wrapped around the projection, it runs for **every namespace**: two
  events (created, published) count 4, each applied once by `summaries`
  and once by `notifications`.
- Generic over `Tx`, `Measured` runs in any unit of work. An interceptor
  that writes in the transaction names it (part 8 has one).
- The intercepted projection keeps its namespaces, their `REPLAY` and
  their `batch`; events a namespace skips on replay skip its interceptors
  too. `updated` is not intercepted: it is not part of the unit of work.
- A pass-through interceptor costs nothing measurable; `Measured` costs
  its two clock reads (about 50 ns).

## The complete asynchronous wiring

```rust
// src/config.rs
pub fn start_in_memory()
-> Running<impl PostCommands, impl PostQueries + PostSubscriptions, InMemoryPostSummaryRepository, PostEvents> {
    let store = PostStore::default();
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let app = Cqrs::new(Events::new(store.clone()))
        .commands(|events| PostCommandGateway::new(posts_over(&store), events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .build();

    Running {
        app: Arc::new(app),
        summaries,
        notifier,
        audit,
        metrics,
    }
}
```

The stores and the services are created here; the unit makes the
publisher from the events, gives the command gateway its unit of work,
starts a processor per namespace, makes the queries subscribable, and
infers every type. The ports say what comes out.

Next: [8. PostgreSQL](08-postgres.md).

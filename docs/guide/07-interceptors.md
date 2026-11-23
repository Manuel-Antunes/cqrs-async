# 7. Interceptors

Logging, metrics, auditing and authorization apply to every message, not
to one use case, so they stay out of the handlers. Two kinds of
interceptor, as in Axon:

- **dispatch interceptors** (`Interceptor<M>`) around a gateway: every
  command or query, with its metadata, before and after its handler;
- **handler interceptors** (`EventInterceptor<E, Tx>`) around an event
  handler, inside its unit of work, with its processing context.

Both wrap with `Intercepted::new(inner, interceptors)`, and a tuple chains
several, the first outermost.

## Auditing every command

```rust
// src/infrastructure/observability.rs
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    pub command: &'static str,
    pub correlation_id: Option<String>,
    pub succeeded: bool,
}

#[derive(Debug, Default)]
pub struct AuditLog(Mutex<Vec<AuditEntry>>);

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
let app = Cqrs::new(store, Arc::new(InMemoryTokenStore::default()))
    .commands(|events| PostCommandGateway::new(posts, events))
    .intercept(Audit(Arc::clone(&audit)))     // Intercepted<PostCommandGateway<..>, Audit>
    // ...
    .build();

let mut metadata = Metadata::new();
metadata.insert("correlation_id".into(), "req-1".into());
app.commands().send_with(CreatePost { title: "Audited".into() }, metadata).await?;
assert_eq!(audit.entries()[0].correlation_id.as_deref(), Some("req-1"));
```

`Intercepted` is a gateway: `CommandGateway<C>` for every command the
inner gateway handles, so it is still `impl PostCommands`, and nothing
downstream changes.

### Refusing a command

An interceptor refuses by returning an error without calling `next`. To
return a typed error, it requires the message's error to accept it:

```rust
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
#[derive(Debug, Default)]
pub struct ProjectionMetrics {
    handled: AtomicU64,
    busy_us: AtomicU64,
}

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
            Ok(()) => { self.0.handled.fetch_add(1, Ordering::Relaxed); }
            Err(error) => tracing::warn!(position = event.position, %error, "projection failed"),
        }
        result
    }
}
```

```rust
let projection = PostSummaryProjection::new(Arc::clone(&summaries), Arc::clone(&updates));
    // in the builder:
    .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
```

- `next` is the rest of the chain (more interceptors, then the handler).
  `next.proceed(context)` runs it in the same unit of work; not calling it
  skips the event, which then counts as handled.
- Generic over `Tx`, `Measured` runs with any token store. An interceptor
  that writes in the transaction names it (part 8 has one).
- The intercepted projection keeps its namespace and its `REPLAY`; events
  a handler skips on replay skip its interceptors too.
- A pass-through interceptor costs nothing measurable; `Measured` costs
  its two clock reads (about 40 ns).

## The complete asynchronous wiring

```rust
// src/config.rs
pub fn start() -> Running<impl PostCommands, impl PostQueries + PostSubscriptions> {
    let store = PostStore::default();
    let posts = Arc::new(EventSourcedPostRepository::new(Arc::clone(&store)));
    let updates = Arc::new(PostUpdates::default());
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostSummaryProjection::new(Arc::clone(&summaries), Arc::clone(&updates));
    let app = Cqrs::new(store, Arc::new(InMemoryTokenStore::default()))
        .commands(|events| PostCommandGateway::new(posts, events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .policy(NotifyOnPublish::new(Arc::clone(&notifier)))
        .subscriptions(updates)
        .build();

    Running { app: Arc::new(app), summaries, notifier, audit, metrics }
}
```

The stores and the services are created here; the unit makes the events
and the publisher from the stores, gives the command gateway its
publisher, infers every type, and the ports say what comes out.

Next: [8. PostgreSQL](08-postgres.md).

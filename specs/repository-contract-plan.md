# Plano: contrato de repositório, `save` e a transação no contexto

Decidido em 2026-11-25, depois da auditoria da API. Revê a seção 7.3 do
[plano de migração](migration-plan.md) ("a biblioteca não define trait de
repositório nenhum"): a biblioteca passa a oferecer um contrato **opcional**
de repositório. A porta própria da aplicação continua válida.

Branch de trabalho: `repository-contract`. Nada é commitado sem pedido.

## O que muda para quem usa

Antes:

```rust
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(&self, command: &PublishPost, ctx: &mut CommandContext<PostEvent>, tx: &mut R::Tx)
        -> Result<(), BlogError>
    {
        let post = self.posts.find(command.id, tx).await?.ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, tx).await?;
        Ok(())
    }
}
```

Depois:

```rust
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(&self, command: &PublishPost, ctx: &mut CommandContext<PostEvent, R::Tx>)
        -> Result<(), BlogError>
    {
        let post = self.posts.find(&command.id, ctx.tx()).await?.ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, ctx.tx()).await?;
        Ok(())
    }
}
```

Sem transação, o handler é o mesmo com `ctx: &mut CommandContext<AccountEvent>`.
O commit (append, commit da transação, anúncio) continua sendo da unidade de
trabalho, depois que o handler retorna.

## Decisões

| # | Decisão |
|---|---|
| R1 | **A transação mora no contexto.** `CommandContext<E, Tx = ()>` é dono da transação da unidade; o handler a alcança com `ctx.tx()`, como o handler de evento com `ProcessingContext` (que também passou a `tx()`). O terceiro argumento de `handle` (`tx: &mut Tx`) sai. |
| R2 | **O modo da unidade vem do tipo do contexto.** `CommandContext<E>` é `Autocommit`; `CommandContext<E, Tx>` é `InTransaction<Tx>` (inclusive `CommandContext<E, ()>`, que é a unidade de memória com vários agregados). `UnitMode`, `Autocommit`, `InTransaction` e `UnitOfWork` não mudam. |
| R3 | **Contrato `Repository<A>`** em `cqrs::aggregate` (e no `prelude`): `type Tx`; quem implementa escreve `find(id, tx)` (obrigatório) e `save(entity, tx)` (vazio por padrão), que recebem só a transação. Não há método provido que receba o contexto: o handler chama `ctx.commit(entity)` e depois `repository.save(&entity, ctx.tx())`. (Revisto: `find` deixou de receber o contexto; a inversão `save`/`persist` foi testada e desfeita; por fim o `save` provido saiu e o hook ficou com o nome `save`.) |
| R4 | **`AggregateLoader<A, S, N = (), Tx = ()>` implementa `Repository<A>` para qualquer `Tx`** (ele lê fora da transação e não grava nela). `new` devolve `Tx = ()`; `in_transaction::<Tx2>()` troca. Fecha o furo de o padrão do blog não compilar sobre transação de banco. |
| R5 | **Carregar num handler:** `find(id, ctx.tx())`. Como `find` não recebe o contexto, um agregado montado por tags que a decisão só lê, ou que pode não existir, é lido com `AggregateLoader::load_in(id, ctx)`, que continua: é o que faz a leitura (inclusive a ausência) condicionar as escritas. `load(&id)` fica como leitura sem contexto (testes, ferramentas, ler outro log). |
| R6 | **`ctx.record` passa a se chamar `ctx.commit`**: commitar uma versão da entidade (os eventos do contexto viram os dela), como no `commit()` do NestJS CQRS, sem relação com o commit do banco, que continua sendo da unidade. A mensagem de eventos não commitados diz "never committed". |
| R7 | **Eventos pendentes ao retornar `Ok` viram erro em qualquer build**, não só `debug_assert!`. Sem isso o `save` não protege de nada. |
| R8 | **As condições de uma unidade são checadas juntas, na primeira escrita.** Corrige a escrita obsoleta com dois agregados por tag. |
| R9 | **Sem tipo `StateStored` de duas closures.** Em Rust estável não dá para exigir `Send` do future de um `AsyncFn` sem `Box<dyn Future>`; implementar o trait (dois métodos: `find` e `save`) dá o mesmo trabalho. |

## Formas exatas

Validadas num protótipo isolado (compila, futures `Send`, um corpo de handler
para repositório de estado, loader com transação e loader sem).

```rust
// crates/cqrs/src/command_handler.rs
pub struct CommandContext<E, Tx = ()> { /* events, metadata, condition, writes, */ tx: Tx }

impl<E> CommandContext<E> {            // Tx = ()
    pub fn new() -> Self;
    pub fn with_metadata(metadata: Metadata) -> Self;
}
impl<E, Tx> CommandContext<E, Tx> {
    /// What a unit of work hands a run of a handler.
    pub fn in_transaction(metadata: Metadata, tx: Tx) -> Self;
    /// The unit's transaction, to write in (it commits with the events).
    pub fn tx(&mut self) -> &mut Tx;
    /// What the run recorded, and the transaction back: what the unit appends in.
    pub fn into_parts(self) -> (Vec<Write<E>>, Tx);
    // append, metadata, pending, writes, condition, source, depend_on, record, take_writes: as today
}

pub trait CommandHandler<C: Command + Sync>: Send + Sync {
    type Event: DomainEvent;
    type Unit: UnitMode;
    fn handle(
        &self,
        command: &C,
        ctx: &mut CommandContext<Self::Event, <Self::Unit as UnitMode>::Tx>,
    ) -> impl Future<Output = Result<OutputOf<C>, ErrorOf<C>>> + Send;
    fn conflict_retries(&self) -> u32 { 8 }
}
```

```rust
// crates/cqrs/src/repository.rs (novo)
pub trait Repository<A: Aggregate>: Send + Sync {
    /// The transaction of the units of work it takes part in (`()`: none).
    type Tx: Send;

    /// The aggregate `id`, at the version it was read at (`None`: it does not exist).
    fn find(&self, id: &A::Id, tx: &mut Self::Tx)
        -> impl Future<Output = Result<Option<Loaded<A>>, StoreError>> + Send;

    /// Keeps `entity`'s state in `tx`. Default: nothing (its events are its state).
    fn save(&self, entity: &Loaded<A>, tx: &mut Self::Tx)
        -> impl Future<Output = Result<(), StoreError>> + Send
    { let _ = (entity, tx); async { Ok(()) } }
}
// + impl para Arc<R>, repassando os dois métodos (#[deny(clippy::missing_trait_methods)]).
```

`dispatch_command` passa a ser:

```rust
let tx = unit.begin().await?;
let mut ctx = CommandContext::in_transaction(metadata.clone(), tx);
let output = handler.handle(&command, &mut ctx).await?;      // erro: ctx cai, tx cai, rollback
if !ctx.pending().is_empty() { return Err(StoreError::backend(/* never recorded */).into()); }
let (writes, mut tx) = ctx.into_parts();
// o resto como hoje: checagem de Autocommit com mais de uma escrita, append_writes, commit, announce
```

`#[command_handler]`: lê o evento e, se houver, a transação dos argumentos de
tipo de `CommandContext<..>`; não acrescenta mais `_: &mut ()`; um terceiro
argumento em `handle` é erro de compilação com a mensagem
"the unit's transaction is in the context: take `ctx: &mut CommandContext<Event, Tx>` and use `ctx.tx()`".

### R8: `append_writes`

Hoje cada escrita checa só a própria condição e o `after` das seguintes é
empurrado para a última posição gravada, o que pula o que outros gravaram no
meio. Passa a ser:

1. `unit = AppendCondition::both(..)` de todas as condições das escritas
   (`or` dos critérios, menor `after`), calculada antes da primeira.
2. A primeira escrita vai com `unit` inteira.
3. Cada escrita seguinte vai com os critérios de `unit` e
   `after = posição do último evento gravado pela unidade` (os eventos da
   própria unidade não contam; o que outros gravaram entre uma escrita e outra
   conta).
4. Sem nenhuma condição, nada muda. Com uma escrita só, nada muda.

## Etapas

Cada etapa começa pelo teste que falha e termina com `cargo test --workspace`
verde.

1. **R7.** `crates/cqrs/tests/stateful.rs`: o teste
   `forgetting_to_save_panics_in_debug_builds` vira um que espera erro em
   qualquer build. Trocar o `debug_assert!` de `dispatch_command`.
2. **R8.** Em `crates/cqrs/tests/dcb.rs`: dois agregados por tag carregados
   com `load`, um terceiro grava no segundo entre a leitura e o commit, o
   commit dos dois falha com `ConditionFailed`; e o caso "leitura por critério
   depois do primeiro `commit`". Depois, `append_writes`.
3. **R1 e R2.** `CommandContext`, `CommandHandler`, `dispatch_command`, o
   macro, e todos os handlers que hoje recebem `tx` (testes do core, do
   postgres, exemplos, benchmarks, doctests, `tests/ui`).
4. **R3, R4, R5.** `repository.rs`, o parâmetro `Tx` do loader, `find`, saída
   do `load_in`.
5. **Exemplos.** Blog: `PostRepository: Repository<Post>` só com `next_id`;
   adaptadores; handlers com `ctx.commit` e `save`. Bank e o quickstart de `lib.rs`:
   `ctx.commit(entity)` direto, porque o loader não guarda estado.
6. **Documentação e snapshots.** Trechos de `docs/content` e do `README.md`
   que a mudança tornou falsos; `CHANGELOG.md`; `public-api.txt` dos crates
   tocados; `tests/ui/*.stderr`.

## Fora deste plano

Ficam para depois, cada um com seu teste antes:

- Leitor do log no PostgreSQL: horizonte de gap por `pg_snapshot_xmax` e
  preenchimentos além do limite da página (perda de evento).
- Dono do claim igual entre réplicas em contêiner; claim perdido e recuperado
  tratado como renovação.
- Reset (`replay_until`, dois commits, dead letters pendentes).
- Deadlock 40P01 como conflito que se refaz; snapshots sem `log` na chave.
- Corte de `EventPublisher`/`Pending`; suíte de conformidade exportada.
- Portão de benchmark desta mudança (A/B em máquina parada): o caminho do
  comando foi tocado.

---

# Parte 2: o repositório vem da engine, a política de snapshot vem do agregado

Decidido em 2026-11-25, em cima da parte 1 (já implementada no branch, sem
commit). Modelo: no Axon 5 declaram-se a engine de eventos e o token store, e
o repositório de cada entidade sai deles; `@Snapshotting` é anotação da
entidade, e o snapshot é tirado durante a carga.

## O que muda para quem usa

Antes (blog, em memória):

```rust
fn posts_over(store: &PostStore) -> EventSourcedPostRepository<PostStore, InMemorySnapshots<Post>> {
    EventSourcedPostRepository::new(store.clone()).with_snapshots(InMemorySnapshots::default(), SNAPSHOT_EVERY)
}
let app = Cqrs::new(Events::new(store.clone()))
    .commands(|events| PostCommandGateway::new(posts_over(&store), events))
```

Depois:

```rust
#[cqrs::aggregate(name = "post", event = PostEvent, snapshot(after_events = 50))]
pub struct Post { .. }

let app = Cqrs::new(Events::new(store))
    .commands(|events| PostCommandGateway::new(events.repository::<Post>(), events))
```

O store, onde ficam os snapshots e o tipo da transação saem dos tipos da
engine. A struct `EventSourcedPostRepository` deixa de existir.

## Decisões

| # | Decisão |
|---|---|
| E1 | **A política de snapshot é do agregado.** `#[cqrs::aggregate(.., snapshot(after_events = 100, after_sourcing = "5s"))]` escreve `impl Snapshotting for Post`. Sem `snapshot(..)`, o macro escreve o mesmo trait dizendo "sem snapshots". |
| E2 | **`SnapshotPolicy`**: `after_events` (o `every` de hoje) e `after_sourcing` (a carga demorou mais que isso), um ou os dois; qualquer um atingido tira o snapshot. |
| E3 | **O snapshot continua sendo tirado na carga**, como hoje e como no Axon. Nunca no `save`: seria do estado depois de eventos ainda não gravados, e uma unidade desfeita deixaria o snapshot à frente do stream. |
| E4 | **A engine diz, em compilação, de quais agregados sabe guardar snapshot** (`KeepSnapshots<A, Mode>`): em memória, os `Clone`; no PostgreSQL, os `Serialize + DeserializeOwned`. Um agregado com `snapshot(..)` numa engine que não sabe guardá-lo não compila. Um sem `snapshot(..)` serve em qualquer engine. |
| E5 | **`events.repository::<A>()`** (em `Events`) devolve o `AggregateLoader` do agregado: store da engine, snapshots da engine, política do agregado, `Tx` da engine (`M::Tx`). `InlinePublisher::repository::<A>()` faz o mesmo com `Tx = ()`. Quem quiser o caminho sem transação num banco transacional pede: `.in_transaction::<()>()`. |
| E6 | **`with_snapshots(store, política)` fica** para a montagem à mão, aceitando `impl Into<SnapshotPolicy>`; um inteiro continua valendo "a cada N eventos", então as chamadas de hoje compilam. |
| E7 | **`Postgres::snapshots::<A>()`** entra na fachada. |
| E8 | **Blog:** a porta é `PostRepository: Repository<Post>`, com `next_id` como mais um método do mesmo repositório (sem struct nem porta de ids à parte; revisto no mesmo dia). `SeaOrmPostRepository` implementa os dois no mesmo arquivo. Para o repositório da engine não há struct do blog: a porta é implementada direto no `AggregateLoader<Post, ..>`, no mesmo arquivo da porta, com o id tirado do log (posição seguinte à do último evento: crescente, não consecutivo; duas criações simultâneas dão conflito na segunda). |

## Formas exatas

Validadas num protótipo isolado (os tipos fecham; o caso que não deve compilar
dá E0277; `with_snapshots(store, 10)` continua compilando).

```rust
// crates/cqrs/src/snapshot.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotPolicy { after_events: Option<u64>, after_sourcing: Option<Duration> }
impl SnapshotPolicy {
    pub const fn after_events(events: u64) -> Self;            // panics at 0
    pub const fn after_sourcing(time: Duration) -> Self;
    pub const fn or_after_events(self, events: u64) -> Self;   // panics at 0
    pub const fn or_after_sourcing(self, time: Duration) -> Self;
    /// Whether a load that replayed `replayed` events in `sourcing` keeps a snapshot.
    pub fn reached(&self, replayed: u64, sourcing: Duration) -> bool;
    /// Whether loads must be timed.
    pub const fn times_sourcing(&self) -> bool;
}
impl From<u64> for SnapshotPolicy;                             // after_events

/// Markers of `Snapshotting::Mode`.
pub struct Snapshots;
pub struct NoSnapshots;

/// How the aggregate is snapshotted: written by `#[cqrs::aggregate]`.
pub trait Snapshotting: Aggregate {
    type Mode;
    const POLICY: Option<SnapshotPolicy>;
}

/// What an event store can keep snapshots of.
pub trait KeepSnapshots<A: Aggregate, Mode> {
    type Snapshots: SnapshotStore<A>;
    fn snapshots(&self) -> Self::Snapshots;
}
impl<A: Aggregate, S> KeepSnapshots<A, NoSnapshots> for S { type Snapshots = (); .. }
impl<A: Aggregate + Clone> KeepSnapshots<A, Snapshots> for InMemoryEventStore<A::Event> { type Snapshots = InMemorySnapshots<A>; .. }
// crates/cqrs-postgres:
impl<A, U> KeepSnapshots<A, Snapshots> for PostgresEventStore<A::Event, U>
where A: Aggregate + Serialize + DeserializeOwned { type Snapshots = PostgresSnapshotStore<A>; .. }
```

```rust
// crates/cqrs/src/streaming/mod.rs
impl<E, S, C, M> Events<E, S, C, M> {
    pub fn repository<A>(&self) -> AggregateLoader<A, S, <S as KeepSnapshots<A, A::Mode>>::Snapshots, M::Tx>
    where A: Snapshotting<Event = E>, S: Clone + KeepSnapshots<A, A::Mode>, M: TransactionManager;
}
```

O loader troca `snapshot_every: u64` por `policy: Option<SnapshotPolicy>` e só
mede o tempo da carga (`std::time::Instant`) quando a política pede.

## Etapas

Cada etapa começa pelo teste e termina com `cargo test --workspace` verde.

1. **E2, E6.** `SnapshotPolicy`, o loader com `policy`, `with_snapshots` com
   `impl Into<SnapshotPolicy>`. Testes: `after_sourcing` (com duração zero,
   qualquer carga que reaplicou algo tira snapshot) e os dois combinados.
2. **E1.** `Snapshotting`, os marcadores e o `snapshot(..)` do macro (erros de
   uso em `tests/ui`).
3. **E4, E5.** `KeepSnapshots`, `Events::repository`, `InlinePublisher::repository`.
   Testes: o repositório da engine tira snapshot pela política do agregado; um
   agregado com `snapshot(..)` que não é `Clone` não compila em memória.
4. **PostgreSQL (E4, E7).** O impl de `KeepSnapshots`, `Postgres::snapshots`, um
   teste no banco: `pg.events(..).repository::<A>()` grava em `cqrs_snapshots`.
5. **Blog (E8).**
6. **Documentação, `CHANGELOG.md`, snapshots de API.**

## Fora desta parte

- Snapshot gravado a cada `save`, na transação (a linha de estado como
  snapshot sempre atual): cabe como um modo da política, só em engine
  transacional. Fica para depois.
- Transação preguiçosa (a unidade só abre a transação quando alguém a pede),
  que acabaria com os dois modos `Autocommit`/`InTransaction`.
- Componentes em memória participando de qualquer transação (engine em
  memória com transação do PostgreSQL).

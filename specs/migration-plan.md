# Plano de migração: de PoC para biblioteca

Estado em 2026-11-24, branch `lib-migration`. Este plano leva o workspace
(`cqrs`, `cqrs-derive`, `cqrs-postgres`) de prova de conceito a uma
biblioteca publicável: corrige os problemas encontrados na revisão, remove
o que está duplicado, tira da biblioteca as decisões que são da
infraestrutura do usuário e revisa a API pública para que seja idiomática.

Ele assume que quem executa não leu as conversas que o produziram: cada
item diz o que está errado, onde, e o que fazer. As fases 0 a 7 foram
feitas (com os desvios anotados em cada item); restam a revisão da
documentação, as Fases 8 e 9 e a Fase 10, nova.

## Princípios

1. **Uma biblioteca, não um framework.** Conexão, transação, schema,
   repositórios, serialização e runtime são do usuário. A biblioteca os
   empresta (`&mut Tx`) e nunca os escolhe por ele. O que ela entrega
   pronto (store PostgreSQL, store em memória, integração com SeaORM) são
   adaptadores opcionais, que se adaptam ao sistema do usuário e não o
   contrário. O core não depende de nenhum banco, ORM ou broker.
2. **Compile-time primeiro.** Roteamento de mensagens, cadeias de
   interceptors, grupos de handlers e a transação em que cada componente
   participa são verificados pelo compilador. Onde o Axon falha no startup
   ("A connection executor must be present in the processing context"),
   aqui não compila.
3. **`dyn` é permitido no encanamento**, quando remove parâmetros de tipo
   ou impls de repasse, e só se o portão de benchmark passar. Cada troca
   para `dyn` entra em um commit próprio, para poder ser revertida sozinha.
4. **Portas pequenas, por capacidade.** Cada componente exige só o que usa.
   Quem implementa um adaptador implementa só as capacidades que o banco
   dele oferece.
5. **Um jeito de fazer cada coisa.** Onde há dois caminhos (escrita,
   dispatch, subscriptions), fica um. Atalhos são açúcar sobre o caminho
   explícito, nunca uma segunda implementação.
6. **Teste antes da correção.** Todo bug começa por um teste que falha.

## Referências de desenho

- **Watermill (Go)** é o modelo de "biblioteca, não framework": um núcleo
  de dois traits (publicar e assinar), adaptadores que recebem o
  `*sql.DB` ou a `*sql.Tx` do usuário, `SchemaAdapter`/`OffsetsAdapter`
  para o layout das tabelas, `InitializeSchema: false` para as migrations
  do usuário, e o forwarder (outbox) para publicar no broker a partir da
  transação do negócio. A camada CQRS dele não tem agregado nem
  repositório: domínio e persistência são do usuário.
- **Axon Framework 5** é o modelo dos conceitos: `ProcessingContext` com
  recursos e fases, `TransactionManager` separado dos stores, componentes
  que pedem o recurso da transação corrente (`TransactionalExecutor<T>`),
  repositórios de estado com load/persist fornecidos pela aplicação
  (`SimpleRepository`), handlers de criação estáticos. O que não se copia:
  recursos resolvidos em runtime e a infraestrutura pensada em torno de um
  servidor próprio.
- **O que nos diferencia dos dois** fica no centro: o log com posições,
  cursores que entendem gaps, DCB e projeções no mesmo commit do token
  (zero eventos perdidos contra o cqrs-es, que perde 171 a 258 por
  rodada). As bordas é que ficam plugáveis.

## Estado

| Item | Status | Commits |
|---|---|---|
| 0.1–0.6 Base: commit inicial, `rustfmt.toml` (120 colunas), CI, cache do sqlx rastreado, benchmark de referência, README/LICENSE/CHANGELOG | feito | `dd23bf1`, `8ea7ff0`, `250cccf`, `18839c6`, `b198dc3`, `d28bf1a` |
| 1.1 Falha de handler inline: política `OnFailure`, padrão `LogFailures` | feito | `38d4c6b` |
| 1.2 Dois enums no mesmo PostgreSQL: log nomeado (`PostgresEventStore::new(pool, "posts")`) | feito | `9df0386` |
| 1.3 Dead letters: consulta que falha não ultrapassa os parqueados; `PostgresDeadLetters`; um trait só, `DeadLetterQueue<E, Tx>`, com entrada e saída da fila na transação do processor | feito | `884bc22`, `84dcbf8`, `4662e11` |
| 1.4 Snapshots no caminho recomendado: o carregador guarda o snapshot depois de um replay longo | feito | `08fed64`, `55699f8` |
| 2.1–2.5 Um caminho de escrita: `AggregateLoader` e `InlinePublisher`; a API antiga saiu | feito | `55699f8` |
| 3.1–3.5 Publisher no construtor do handler; sai a torre `DispatchIn` | feito | `c7a5cc9` |
| 4.1–4.2 Sem `Bus`; um método obrigatório por trait de dispatch | feito | `080c970` |
| 6.8–6.9 Uma migração na forma final; sem `EventCriteria::matches` | feito | `aaa8721` |
| Criação estática: `Post::create(id, ..)` e commit em `New::<Post>(id)`; `Loaded::new` saiu da API pública | feito | `5437e6e` |
| Log de snapshot store que falha; `tracing` sem features padrão | feito | `c3471e1`, `3c978e5` |
| 5.1–5.4 Um mecanismo de subscription (emitters), correto com várias instâncias | feito | `9b58d45` |
| 6.1 Stores como handles | feito | `0e7169d` |
| 6.2 Tupla = grupo em todo lugar, `Isolated` para o inline; métodos de grupo selados | feito | `508bd0c` |
| 6.3 Erros com causa | feito | `486cfdd` |
| 6.4 `work` dividido em roteado e autônomo | feito | `926476c` |
| 7.1 `TransactionManager` | feito | `f3f7b84` |
| 7.2–7.3 `commit_in`, `Committed`, `announce()`, `Loaded::at` | feito | `c30be58` |
| 7.4 Portas por capacidade | feito | `2b0fab8` |
| 7.5 `run(shutdown)` | feito, parcial (ver o item) | `5406485` |
| 7.7–7.9 `MIGRATIONS`, `Codec`, `Forward` | feito | `3712167` |
| 7.10–7.11 Blog no PostgreSQL com SeaORM | feito | `3cbf004` |
| `#[derive(DomainEvent)]` resolve as tags; `#[cqrs::aggregate]`, `#[cqrs::command]`, `#[cqrs::query]` | feito | `185ac64` |
| 7b.1a Namespaces (API) | feito | `562870c` |
| 7b.1b Unidade de trabalho do comando | feito | `99f24ee` |
| 7b.1c `cqrs-sea-orm` | feito | `9f0e243` |
| 7b.3a Posições de outros logs não viram gaps (medido antes e depois) | feito | `dbef27e` |
| 7b.3b `cqrs_postgres::Postgres::new(pool)`; 7b.3d `Units` | feito | `9899e28` |
| 7b.4 Teste `racing_writers...` corrigido | feito | `984145d` |
| Batch por namespace: `#[cqrs::event_handler(namespace, batch = N)]` | feito | `c75b727` |
| D17 Gateways como handlers: `#[command_gateway]`, `#[query_gateway]`, `CommandHandler<C>`, `QueryHandler<Q>` | feito | `f07a3b1` |
| D18 Updates na projeção: `updates(..)`, `updated`, `QueryUpdateEmitter<Self>` | feito | `768ac7d` |
| Portão: A/B contra `5437e6e` depois da 7b, D17 e D18 | feito, empate | `263f8cd` |
| Caminhos do PostgreSQL mais rápidos: `LoadStream::replay`, conflito sem segunda leitura, append sem `Value`, leitura de página sem sondas inúteis | feito; todas as medidas do portão passam | `c02e963` |
| MSRV por crate (1.89 núcleo, 1.94 Postgres) verificado; rustdoc sem avisos; decisões: spike B adiado, `LISTEN/NOTIFY` separado, lote padrão 1 | feito | `3979e35` |
| 8: D20 `CommandContext`, D22 locks que sobrevivem a pânico | feito | `2f668bc` |
| 8: D21 envelope sem cópias por evento (`Cow` no tipo, `Metadata` compartilhada) | feito | `f74120f` |
| 8.1 / D19 módulos públicos e `prelude`; macros por `__private` | feito | `94a3719` |
| 8.4 checklist: `Debug` em tudo (lint ligado), `#[non_exhaustive]`, setters das opções, `ProcessingFailure: Error`, `#[must_use]`, `EventChain` selado, `try_from` no Postgres | feito | `10df476` |
| 8.3 `EventEnvelope::aggregate_id::<A>()` | feito | `e1aeccf` |
| 8.6 mensagens dos macros com `trybuild`; doc de cada macro mostra o que ele escreve; `CommandContext::append` | feito | `30db3af` |
| 8.7 quickstart compilado, rustdoc sem "PoC" e sem os parênteses do Axon, esboços em dia | feito | `ce7adf1` |
| 8: snapshot do `cargo public-api` (cqrs, cqrs-postgres, cqrs-sea-orm) revisado e checado no CI (nightly) | feito | `36526bf` |
| D23 derives `Command`/`Query`/`AggregateRoot` removidos | feito | `4702adb` |
| `LISTEN/NOTIFY`: `Events::committed_elsewhere()` no núcleo, `Postgres::listen(&events)` no `cqrs-postgres` (aviso depois do commit, agrupado; nunca dentro da transação do append); o blog no PostgreSQL escuta | feito | `374dbec` |
| Dead letters nos namespaces de uma projeção: `ProjectionProcessors::with_dead_letters`, `retry_dead_letters`, `CqrsBuilder::projection_with` | feito | `7b5b78e` |
| 9: pacotes `cqrs-async`, `cqrs-async-macros`, `cqrs-async-postgres`, `cqrs-async-sea-orm` em `0.1.0-alpha.1`, código continua `cqrs::`/`cqrs_postgres::`/`cqrs_sea_orm::` (`[lib] name`, D24); metadados, `include` com `.sqlx` e migrações; `deny.toml`; CI com `cargo deny` e `cargo publish --dry-run`; projeto de fora compila e roda o quickstart só com os pacotes | feito | `c315a04` |
| 9: portão antes da primeira release (A/B `c315a04` × `c02e963`): todas as medidas passam, empate dentro da variação; `performance.md` com esses números | feito | `f39b820` |
| 10.5 requeue com política: `EnqueuePolicy`/`EnqueueDecision` (forma do Axon 5, mais `Retry`), `ParkAfter` padrão, letter com `attempts`/`parked_at`/`last_touched`/`diagnostics`, `requeue_in`, colunas no PostgreSQL | feito | `11092b9` |
| 10.4 mensagens com atraso: `CommandSchedule<C, Tx>` (forma do `CommandScheduler` do Axon 4), `schedule_in` na unidade de trabalho, `deliver_scheduled` com lease/backoff/desistência, `InMemorySchedule`, `PostgresSchedule` (`cqrs_scheduled`, relógio do banco, `SKIP LOCKED`) | feito | `083974e` |
| 10.1 portas de broker: `Subscribe`/`Consume`/`Incoming` no núcleo (forma do `SubscribableEventSource`); crate `cqrs-async-amqp` (lapin) com `AmqpPublisher`/`AmqpSubscriber` nos presets do Watermill | feito | `6b9fda4` |
| 10.2 fan-out: um `Forward` por destino, cada um no seu ritmo (teste) | feito | `111899a` |
| 10.3 fan-in (opção B): `#[event(fan_in)]`, `FanIn`/`Events::fan_in`, `PostgresFanIn`/`Postgres::fan_in`, `Events::follow` | feito | `3ff1eef` |
| `examples/bank`: APIs REST (axum), RabbitMQ entre duas aplicações, fan-in; portão depois da Fase 10 (A/B × `f39b820`): sem regressão, dois limites perdidos por 0,01 numa máquina carregada | feito | `b1ea047` |
| 9: `repository`/`homepage` nos manifestos, README e CHANGELOG da `0.1.0-alpha.1`, repositório no GitHub com o site no Pages | feito | (este commit) |
| **Fase 8 concluída.** 7b.5: o site Docusaurus (`docs/content`, `docs/src/pages/index.js`) reescrito contra a API final; `docusaurus build` sem links quebrados | feito (arquivos do usuário, sem commit) | — |

O que fica, e por quê:

- **Publicar no crates.io** (`cargo publish --workspace --exclude blog --exclude bank --exclude
  cqrs-benchmarks --exclude cqrs-versus`, na ordem macros → núcleo → adaptadores, que o cargo segue): decisão
  do usuário; reserva os nomes, livres hoje. Depois dela, ligar o `cargo semver-checks` no CI.
- **Portão com a máquina parada:** o último (depois da Fase 10) foi numa máquina com carga 4,3–4,9 e errou
  dois limites por 0,01 (tempo até concluir 0,78, snapshot p50 0,62), sem regressão contra o código da release,
  que erra mais sob a mesma carga.
- **Cenários novos no versus** (repositório com estado, subscriptions entre instâncias): melhoria, não
  bloqueia a release.
- **O Tokio dentro do processor** (7.5 parcial): mantido e documentado (event processing).

Resolvidos (antes listados aqui): o CI roda no primeiro push; o MSRV por crate está no README; o risco do 1.2
(posições de outros logs como gaps) foi medido e resolvido na 7b.3a; o portão decidiu nas rodadas A/B; a
documentação foi reescrita no site (7b.5) e cobre a Fase 10.

## Portão de benchmark

A regra é não cair em relação à biblioteca concorrente (cqrs-es 0.5 com
postgres-es 0.5). O portão roda ao fim das Fases 5, 6 e 7, e depois de cada
commit que introduz `dyn` ou mexe no caminho do append.

```text
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run --release -p cqrs-versus -- 3
cargo bench -p cqrs-benchmarks --bench micro
```

| Medida | Base (docs/performance.md) | Passa se |
|---|---:|---|
| open account, nós ÷ eles | 1,38× | ≥ 1,10× |
| mixed load, cmd/s | 1,57× | ≥ 1,25× |
| mixed load, p99 | 0,63× | ≤ 0,76× |
| mixed load, tudo pronto | 0,64× | ≤ 0,77× |
| hot account, cmd/s | 1,15× | ≥ 1,00× |
| rebuild do read model | 20,8× | ≥ 16× |
| replay de 5000 eventos, p50 | 0,79× | ≤ 0,95× |
| com snapshot a cada 100, p50 | 0,51× | ≤ 0,61× |
| eventos ou dinheiro perdidos | 0 | 0 |

**Como rodar para decidir.** Na máquina da migração, com load average entre
10 e 22, o mesmo binário variou de 2700 a 4500 cmd/s em open account, e as
medidas que falham trocam de uma rodada para a outra. Duas regras:

1. Rodar com a máquina parada (só o Docker). É o único jeito de o portão
   ser absoluto.
2. Para comparar duas versões do código, rodar os dois binários
   alternados, mesma carga, três rodadas de cada (método de
   [gate-log.md](benchmarks/gate-log.md)). No A/B de 2026-11-23, o código
   atual e o da Fase 0 empatam: open account 1,20× contra 1,15×, mixed
   1,32× contra 1,30×, "tudo pronto" 0,77× contra 0,78×.

Os micro-benchmarks são informativos (gateway 12,7 ns, quatro
interceptors 10,8 ns, ciclo de comando 1,01 µs nesta máquina): uma
regressão ali precisa de explicação no commit, mas quem decide é a tabela.

## Decisões

| # | Decisão | Situação |
|---|---|---|
| D1 | Nome dos crates | **Decidido:** `cqrs-async`, `cqrs-async-macros`, `cqrs-async-postgres`. Livres em 2026-11-23; só ficam garantidos na primeira publicação. |
| D2 | Vários enums de evento no mesmo banco | **Feito:** log nomeado por store. |
| D3 | Handler inline que falha | **Feito:** política `on_failure`, padrão registrar. |
| D4 | Mecanismo único de subscription | **Implementado com a recomendação** (emitters; `LiveQueries` saiu, `9b58d45`). Reverter custa um commit. |
| D5 | Modo sem `tokio` | Resolvido pela 7.5: processors viram futures, e `tokio` só dá o atalho `spawn`. |
| D6 | Publisher no construtor do handler | **Feito** (Fase 3). |
| D7 | Quem faz o commit da transação | **Revisto em 2026-11-24:** o framework, nas duas pontas, como o Axon. Leitura: o processor abre a unidade de trabalho com o `TransactionManager` e a fecha (já era assim). Escrita: o gateway abre a unidade do comando, o handler e os repositórios recebem `&mut Tx` direto (sem wrapper), e o framework grava os eventos na mesma transação e faz o commit (7b.1b). O tipo da transação continua do usuário (o `TransactionManager` dele). Substitui o `commit_in` da 7.2. |
| D8 | Serialização | **Decidido:** serde é o contrato, exigido só pelos adaptadores; `Codec<T>` mínimo só onde o adaptador grava bytes (7.8). |
| D9 | Onde mora a integração com SeaORM | **Decidido:** a transação comum no PostgreSQL é a do sqlx (`PostgresTransactions`); o SeaORM entra pela vista `SeaOrm<'a>` (`ConnectionTrait` sobre `&mut PgTransaction`), no crate `cqrs-async-sea-orm` (só pelo MSRV 1.94). O `DatabaseTransaction` do SeaORM não serve como tipo comum: guarda um `PoolConnection` privado e não empresta a conexão sqlx (verificado no 2.0.4). Rodar o SQL da biblioteca pelo SeaORM foi descartado (duplicaria o SQL e perderia a checagem do `query!`). |
| D10 | O que o exemplo do blog mostra | **Implementado com a recomendação** (`3cbf004`): o `PostRepository` ganhou `next_id` e `save`, e cada adaptador decide como guarda. |
| D11 | O que é uma tupla de handlers | **Decidido:** um grupo, em todo lugar; isolamento inline é `Isolated((a, b))` (`508bd0c`). |
| D12 | Pasta `policies` | **Decidido:** saiu; um efeito colateral é um namespace da projeção com `REPLAY = false`. O `.policy(..)` do builder sai em 7b.1a. |
| D13 | Namespaces | **Decidido:** uma projeção por raiz (`#[cqrs::projection(name, events, namespaces(..))]` na struct), um `#[cqrs::event_handler(namespace = ..)] impl` por namespace, checados nos dois sentidos em compile time. O namespace fica na projeção, não no evento: o domínio não conhece quem o consome. O nome do evento não muda. |
| D14 | Estratégia de posições no PostgreSQL | **Decidido:** manter a ordem global (como o Axon 5: um stream por store, filtros sobre ele). Agora a A (o cursor de um log avança sobre os outros logs, 7b.3a); a B (posições sem gaps com finalizador, como o engine PostgreSQL do Axon) vai a spike e só entra se o portão empatar ou ganhar (7b.3c). Sequência por log descartada: perde a ordem global que o fan-in usa. |
| D15 | Vários agregados num comando sem transação | **Decidido (7b.1b):** erro claro em `Autocommit` com mais de um `record`; com transação, os appends são atômicos. Numa unidade que não desfaz (memória, `TransactionManager::ROLLS_BACK = false`), o handler não é refeito depois de uma gravação parcial: o conflito chega a quem mandou o comando. |
| D16 | Efeito externo no handler de comando | **Decidido:** vai para um namespace de projeção, não para o handler: o handler é refeito depois de um conflito. Documentar no guia. |
| D17 | Forma dos gateways | **Decidido:** o gateway é o handler. Uma struct por lado (`#[cqrs::command_gateway(commands = PostCommands, handles(CreatePost, ..))]`, `#[cqrs::query_gateway(queries = .., subscriptions = .., handles(..))]`), com um `impl CommandHandler<C>` / `QueryHandler<Q>` por mensagem, que dividem as dependências (sem struct por handler, sem `Arc` para repassar o repositório). A mensagem é parâmetro genérico do trait. `handles(..)` é a lista: uma mensagem listada sem impl não compila onde o gateway é declarado (o marcador `Handles<M>`, escrito pelo `#[command_handler]`/`#[query_handler]`, sem bounds). Saem o `Handler` genérico, o `#[derive(Gateway)]` com handlers em campos e os enums de mensagens (`#[derive(Commands)]`/`#[derive(Queries)]`, `DispatchVia`): o trait que o código de fora nomeia (antiga `port`) é gerado pelo gateway. Um enum para transporte volta, se fizer falta, na Fase 10. |
| D18 | Updates das subscription queries | **Decidido:** são da projeção, como no Axon (o emitter vem do contexto do handler, não de um objeto à parte). `#[cqrs::projection(.., updates(FindPost, ListPublished), subscriptions = PostSubscriptions)]` gera um canal por query (o tipo `QueryUpdateEmitter<Self>`, sem campos novos na struct) e o trait de inscrição; o namespace emite em `async fn updated(&self, event, updates: &QueryUpdateEmitter<Self>)` (`updates.emit(\|q: &FindPost\| .., resultado)`, canal escolhido pelo tipo da query; query fora da lista não compila). O `.projection(p)` do `Cqrs` cria os canais, sobe um emissor por namespace com `updated` em cada instância (esperando só o progresso daquele namespace) e torna as queries da unidade inscritíveis. Saem o `.emit(..)`, o `.subscriptions(..)` e o `subscriptions =` do `query_gateway`; o canal de uma query passa a se chamar `QueryUpdateChannel<Q>`, e `#[derive(QueryUpdates)]` com `EmitUpdates` ficam para montagem à mão. Uma projeção por unidade (uma raiz por unidade, D13). |
| D19 | Organização da API (8.1) | **Decidido:** módulos públicos (`aggregate`, `command`, `query`, `event`, `store`, `processing`, `testing`; as subscription queries ficaram em `query`, e `subscribe` do log em `store`) e um `prelude` pequeno; os macros ficam na raiz. |
| D20 | Nome do contexto do handler de comando (8.2) | **Decidido:** `Appender<E>` vira `CommandContext<E>`; o domínio continua recebendo `&mut impl EventAppender<E>`. `Projection` fica com o nome (D12/D13: efeitos colaterais são namespaces dela). |
| D21 | Custos por evento no envelope (8.5) | **Decidido:** mudar antes da release: `event_type` sem alocação no caminho comum e metadata compartilhada entre os eventos de um commit; medido no A/B. |
| D22 | Mutex envenenado (8.4) | **Decidido:** recuperar o guard (`PoisonError::into_inner`) num helper, sem dependência nova. |
| D23 | Derives redundantes | **Decidido:** comandos, queries e agregados se declaram só por atributo (`#[cqrs::command]`, `#[cqrs::query]`, `#[cqrs::aggregate]`); os derives `Command`, `Query` e `AggregateRoot` saíram. Uma forma para cada coisa. `#[derive(cqrs::DomainEvent)]` fica (eventos não têm forma de atributo). |
| D24 | Nome no código | **Decidido:** os pacotes são `cqrs-async*`, mas o código escreve `cqrs::`, `cqrs_postgres::` e `cqrs_sea_orm::` (`[lib] name`). Os macros acham o crate pelo nome do pacote: sem renomear é `::cqrs`; renomeado, a chave escolhida. O crate antigo `cqrs` (0.3.1, 2019) só conflita com quem depender dos dois. |
| D25 | Contrato de repositório e repositório da engine | **Decidido (2026-11-25):** revê a 7.3. `Repository<A>` opcional (`find(id, tx)`, `save(entity, tx)`, só a transação); a transação mora no contexto (`CommandContext<E, Tx>`, `ctx.tx()`, o modo da unidade vem do tipo); `ctx.record` virou `ctx.commit`; `events.repository::<A>()` monta o loader com store, snapshots e transação da engine; a política de snapshot é do agregado (`#[cqrs::aggregate(.., snapshot(after_events, after_sourcing))]`), checada em compilação contra o que o store sabe guardar. Detalhes, revisões e o que ficou de fora em [repository-contract-plan.md](repository-contract-plan.md); o que mudou para quem usa, no `CHANGELOG.md`. |

## Visão geral do que resta

| Fase | O que entrega | Tamanho | Depende de |
|---|---|---|---|
| 5 | Um mecanismo de subscription, correto com várias instâncias | G | D4 |
| 6 | Encanamento: stores como handles, grupos, erros com causa | M | — |
| 7 | A infraestrutura é do usuário | G | 6.1 |
| 7b | Namespaces, unidade de trabalho do comando, posições | G | 7 |
| 8 | API pública idiomática | G | 5–7 |
| 9 | Empacotamento e primeira release | P | 8, D1 |
| 10 | Mensageria: brokers, fan-in, atraso, requeue (nova) | G | 7.9 |

As Fases 5 e 6 não dependem uma da outra. A 7 é o centro desta revisão do
plano e é a que o exemplo com SeaORM valida.

---

## Fase 5 — Um mecanismo de subscription

Hoje há dois: `LiveQueries` reexecuta a query quando um evento relevante
foi projetado; `SubscriptionQueries` recebe o que a projeção emite. Pela
decisão D4 (a confirmar) ficam os emitters.

- **5.1 Remover `LiveQueries`, `RefreshedBy` e uma das duas funções
  `distinct`** ([live_queries.rs](../crates/cqrs/src/streaming/live_queries.rs),
  [updates.rs](../crates/cqrs/src/streaming/updates.rs)). No blog saem as
  impls de `RefreshedBy`, que ninguém usa. `LocalProgress` e
  `GlobalProgress` moram no mesmo arquivo e ficam: 5.2 precisa deles.
- **5.2 Fechar o buraco de várias instâncias.** Um emitter só dispara na
  instância que projetou o evento; um assinante conectado a outra nunca
  recebe a atualização. Proposta: a emissão passa a ser feita, em toda
  instância, por uma tarefa que segue o feed de eventos, espera o
  progresso global da projeção (`GlobalProgress`) e então chama o hook de
  emissão. Com uma instância só, usa o progresso local e o custo é o de
  hoje.
- **5.3 Teste que define o pronto:** duas instâncias sobre o mesmo
  PostgreSQL; assinante na instância B recebe a mudança projetada pela A.
- **5.4 Separar os dois usos de `committed`:** "efeito pós-commit na
  instância que processou" e "emitir atualização" ganham nomes próprios.
- **5.5 Candidato a `dyn`:** o registro de emitters, hoje uma struct com
  `#[derive(QueryUpdates)]` e o trait `UpdatesOf<Q>`. Um registro indexado
  pelo tipo da query removeria o derive e o parâmetro `U`, ao custo de uma
  busca por emissão. Só entra se o portão não cair.
- **Feito assim (5.2, 5.4):** emitir é um hook próprio, `EmitUpdates`, fora
  do `committed`. `emit_updates` o chama em toda instância, para cada
  evento, quando os projections o trataram, em qualquer instância; quem diz
  isso é `ProcessorStatus::handled`, que usa os segmentos da própria
  instância quando ela tem todos e, senão, os cursores gravados de todos
  (lidos a cada `progress_poll`, 50 ms, só enquanto divide). As
  subscriptions esperam os emitters pegarem o ponto de partida, para não
  perder updates. **5.5 não foi feito** (opcional).

Pronto quando: 5.3 passa, o guia de live queries descreve um mecanismo só,
e o portão passa.

## Fase 6 — Encanamento

- **6.1 Stores como handles.** `InMemoryEventStore` e `InMemoryTokenStore`
  passam a ter o `Arc` por dentro e a ser `Clone`, como
  `PostgresEventStore` já é. Saem as impls de repasse para `Arc<T>` de
  `EventStore`, `TokenStore`, `TransactionalTokenStore`, `DeadLetterQueue`
  e `EventFeed`. Ficam as de `Handler`, `Dispatch` e `EventHandler`, que
  usuários de fato compartilham em `Arc`.
- **6.2 Grupos de handlers.** `handle_in` e `committed_in` são métodos
  públicos de `EventHandler` que o usuário nunca deve chamar nem
  implementar: mover para um trait interno, selado. E resolver a tupla com
  dois significados: `(a, b)` é "isolados" como `EventProcessor`
  ([inline.rs](../crates/cqrs/src/inline.rs)) e "tudo ou nada" como
  `EventHandler` ([message.rs](../crates/cqrs/src/message.rs)). Um
  significado para a tupla, um wrapper com nome para o outro.
- **6.3 Erros com causa (`dyn`).** `StoreError::Backend(String)` e
  `HandlerError(String)` jogam fora o erro original. Guardar
  `Box<dyn Error + Send + Sync>` e expor `source()`. `StoreError` deixa de
  ser `PartialEq`: os testes que comparam erros passam a usar `matches!`.
- **6.4 `TrackingProcessor::work`.** Manter o leitor compartilhado (com 14
  segmentos, de 43 mil para até ~497 mil eventos/s em memória). Separar os
  dois modos da função, roteado e autônomo, em duas, e tirar a checagem de
  `REPLAY` duplicada entre `handle` e `handle_in`.

Descartados ou absorvidos: dead letters por `dyn` (a fila é genérica
sobre a transação, e é esse tipo que impede combinar fila em memória com
tokens em banco); unir os traits de token store (vira a 7.1); a feature
`tokio` espalhada (vira a 7.5).

O que fica estático de propósito: as macros de tupla para handlers,
interceptors e upcasters. É o que dá a cadeia de interceptors a ~3 ns por
elo.

## Fase 7 — A infraestrutura é do usuário

O que dita infraestrutura hoje:

1. A transação é do token store (`TransactionalTokenStore::begin/commit`),
   então o `Tx` é escolhido pelo adaptador, não pelo usuário.
2. O append abre a própria transação
   ([event_store.rs](../crates/cqrs-postgres/src/event_store.rs)): não dá
   para gravar o estado do usuário e os eventos no mesmo commit.
3. `EventStore` é um trait de oito métodos (load, append, append_if,
   append_tagged, source, read_all, last_position, read/cursor_at): outro
   banco precisa implementar tudo, até DCB e cursor com gaps.
4. `Cqrs`/`CqrsBuilder` cria o `Events`, chama `tokio::spawn` nos
   processors e controla o ciclo de vida.
5. `migrate()` roda o SQL do jeito dele.

### 7.1 `TransactionManager` separado do token store

```rust
pub trait TransactionManager: Send + Sync {
    type Tx: Send;
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, StoreError>> + Send;
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

- Implementado para o modo em memória (`Tx = ()`) e, no PostgreSQL, por
  `cqrs_postgres::PostgresTransactions(pool)`. **Desvio:** não dá para
  implementar para `PgPool` direto (regra de órfãos: trait e tipo são de
  outros crates).
- Token store, fila de dead letters e handlers são participantes: recebem
  `&mut Tx` e não abrem nem fecham nada. O token store perde `begin` e
  `commit`; `store` e `store_in` viram um método. `TokenUpdate::Untouched`
  vira detalhe interno do processor.
- O processor recebe o gerenciador e os participantes, e o compilador
  exige que todos falem a mesma `Tx`.
- A posse codifica as fases do Axon: `commit` consome a transação (commit),
  soltar sem commit desfaz (on-error), `committed` do handler é o
  after-commit.

### 7.2 Escrita dentro da transação do usuário

```rust
let mut tx = self.db.begin().await?;                              // a transação é dele
self.posts.save(&mut tx, &post).await?;                           // o repositório e o ORM dele
let appended = appender.commit_in(&mut tx, post, &self.events).await?;
tx.commit().await?;                                               // o commit é dele
appended.announce();                                              // processors e subscriptions
```

- `commit` (que abre a própria transação) continua como atalho para quem
  não precisa disso. **Desvio:** não é implementado sobre `commit_in`. No
  PostgreSQL o atalho é um statement só no pool (um round trip); sobre
  `commit_in` seriam três (`BEGIN`, append, `COMMIT`). Os dois caminhos
  dividem a montagem dos envelopes e o cálculo de versão e condição.
- **Feito assim:** `AppendEvents<E, Tx = ()>` diz em que transação o append
  entra (`()` = a do próprio store); `PublishIn<E, Tx>` (`append_in`) e
  `Announce<E>` (`announce`) são os traits do publisher; `Committed` traz o
  agregado depois dos eventos, na versão nova, para o repositório que
  guarda estado salvar na mesma transação.
- O empréstimo impede a biblioteca de fazer commit ou de guardar a
  transação. No Watermill isso é convenção; aqui é o compilador.
- **O aviso depois do commit.** O publisher não sabe quando o usuário faz
  commit, então não pode avisar os processors na hora. `commit_in` devolve
  um valor `#[must_use]` com o que foi gravado, e `announce()` avisa.
  - Com `Events`, esquecer o `announce()` só custa latência: os processors
    também leem por polling (teste
    `a_lost_last_broadcast_is_recovered_by_polling`).
  - Com `InlinePublisher`, os handlers inline rodariam no `announce()`; se
    ele fosse esquecido, não rodariam. **Decidido:** o publisher inline não
    oferece `commit_in`. Um aviso no `Drop` daria falso positivo no caso
    certo (a transação do usuário falhou e o valor é descartado).
- **Retry por conflito** continua funcionando: a transação nasce dentro do
  handler, um conflito desfaz ao soltar, e o gateway roda o handler de
  novo.
- **DCB dentro da transação do usuário:** o append condicional toma locks
  exclusivos dos critérios. Dentro da transação do usuário, eles ficam
  presos até o commit dele. Documentar e medir; o atalho `commit` mantém o
  comportamento de hoje.

### 7.3 Repositórios com estado

**Revisto em 2026-11-25 (D25):** a biblioteca passou a oferecer um contrato
opcional, `Repository<A>`, e a engine entrega o repositório de cada
agregado. O texto abaixo é o desenho anterior.

O usuário implementa o repositório como quiser; a biblioteca não define
trait de repositório nenhum.

- Para o commit, ele precisa entregar um `Loaded<A>` com a versão:
  `Loaded::at(aggregate, version)`. Se o estado guarda a versão do stream,
  o append com `ExpectedVersion::Exact(version)` continua dando a
  concorrência otimista.
- O teste que define o pronto: estado e eventos fazem commit juntos e são
  desfeitos juntos.

### 7.4 Portas por capacidade

```rust
trait AppendEvents<E, Tx>   // grava, dentro da transação
trait LoadStream<E>          // carrega um agregado
trait ReadLog<E>             // lê o log em ordem, com cursor (processors)
trait SourceEvents<E>        // lê por critério (DCB)
```

- O publisher exige `AppendEvents`; o processor e as subscriptions,
  `ReadLog`; decisões por critério, `SourceEvents`. `EventStore` virou o
  guarda-chuva das quatro, implementado para quem tem todas.
- **Desvio:** o `AggregateLoader` exige `LoadStream + SourceEvents`, porque
  `Aggregate::criteria` decide em runtime entre stream e critério. Um store
  sem DCB (DynamoDB) usa o publisher e o gateway, e o repositório do
  usuário carrega à mão e entrega a versão com `Loaded::at`. Separar o
  carregamento por critério em compile time fica para a Fase 8.
- Toca o caminho quente: portão de benchmark obrigatório.

### 7.5 Processors como futures

- `processor.run(shutdown)` devolve um future; o usuário escolhe onde
  rodar (`tokio::spawn`, `JoinSet`, outro runtime).
- `spawn()` fica como atalho, só com a feature `tokio`, e os `cfg` saem de
  `lib.rs` e `unit.rs`.
- **Parcial:** `run(shutdown)` e `progress()` existem e o usuário escolhe
  onde roda o future, mas por dentro o processor ainda usa Tokio (uma
  tarefa por segmento, `mpsc`, `watch`, timers). Os `cfg` ficaram. Tirar o
  Tokio de dentro é reescrever o processor sobre um executor abstrato; só
  vale se aparecer um usuário de outro runtime.

### 7.6 `Cqrs` como açúcar

`Cqrs`/`CqrsBuilder` continuam, mas tudo o que eles fazem é possível sem
eles: o guia mostra primeiro a montagem à mão (store, publisher, gateway,
processors) e depois o builder como atalho.

### 7.7 Schema do usuário

- `cqrs_postgres::MIGRATIONS`: o SQL exposto, para `sqlx migrate`,
  `sea-orm-migration`, refinery ou o que o usuário usar. `migrate()` vira
  opcional.
- Nomes de tabela continuam fixos: o `sqlx::query!` exige SQL literal para
  checar em compilação. Escolher o schema do PostgreSQL por `search_path`,
  como os testes já fazem, e documentar.
- `PgPool` e `PgTransaction` estão na API pública: reexportar `sqlx` e
  documentar que uma troca de versão maior do sqlx é quebra.

### 7.8 Serialização

- **serde é o contrato.** O core não exige serde (`DomainEvent` não pede);
  os adaptadores que persistem exigem `Serialize + DeserializeOwned`. O
  store em memória guarda eventos tipados e não serializa. O cqrs-es, ao
  contrário, exige serde no próprio `DomainEvent`.
- **`Codec<T>` só onde o adaptador grava bytes** (forwarder para broker, um
  eventual store com coluna `bytea`):

  ```rust
  pub trait Codec<T>: Send + Sync {
      fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError>;
      fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
  }
  impl<T: Serialize + DeserializeOwned> Codec<T> for Json { .. }
  impl<T: prost::Message + Default> Codec<T> for Protobuf { .. }   // feature
  ```

  Quem usa serde não escreve nada; protobuf e formatos próprios
  implementam `Codec` para o tipo deles.
- **Protobuf binário não passa por serde.** O formato precisa do número de
  cada campo, que o modelo do serde não carrega. Nenhum crate estabelecido
  grava protobuf binário via serde (`serde-protobuf` 0.8 só lê, com o
  schema em runtime). Os tipos gerados com `pbjson-build` ganham serde no
  JSON oficial do protobuf e cabem no `PostgresEventStore` de hoje.
- **O PostgreSQL fica em JSON:** a coluna `json` é a escolha que deu o
  ganho no replay, e os upcasters reescrevem JSON. Upcasting genérico só
  existe em formatos autodescritivos (JSON, CBOR, MessagePack); documentar
  por adaptador.
- Nome e versão do evento continuam nossos (`#[event(name, version)]`): o
  serde serializa o conteúdo, o envelope é da biblioteca.

### 7.9 O log como outbox

- Um tracking processor cujo handler publica no broker faz o papel do
  forwarder do Watermill: o evento já está no log, na transação do
  negócio, e o processor leva ao broker pelo menos uma vez.
- A biblioteca define uma porta mínima de publicação e um handler
  `Forward<P, C>` genérico sobre ela e sobre o `Codec`. Adaptadores de
  broker (SQS/SNS, AMQP, Kafka) ficam em crates opcionais, com resolvers de
  nome como os do Watermill, e podem vir depois da 0.1.
- Na entrada, o consumidor do usuário chama `gateway.send(cmd)`: a
  biblioteca não precisa de um `Subscriber` próprio.

### 7.10 Integração com SeaORM

Verificado no código do SeaORM 2.0.4 (atual em 2026-11-24): ele usa sqlx
0.9, a mesma versão do `cqrs-postgres`, e as conversões
`From<PgRow> for QueryResult` e `From<PgQueryResult> for ExecResult` são
públicas.

- Uma vista emprestada `SeaOrm<'a>` implementa o `ConnectionTrait` do
  SeaORM sobre `&'a mut PgTransaction`. O usuário escreve com SeaORM na
  transação do processor ou na dele, e a vista não consegue fazer commit
  nem viver além da transação.
- O contrário não funciona: o `DatabaseTransaction` do SeaORM não expõe a
  conexão sqlx de dentro, e os stores perderiam o `sqlx::query!`.
- Onde mora (D9): crate `cqrs-async-sea-orm`. O SeaORM 2 pede Rust 1.94;
  como feature do `cqrs-postgres`, ele herdaria esse MSRV. Começar dentro
  do exemplo e extrair quando a API estabilizar.
- Quem usa sqlx puro não precisa de nada: escreve com `&mut **tx`.

### 7.11 O exemplo do blog com SeaORM

É o teste de aceitação da fase (D10):

- **Escrita:** um `SeaOrmPostRepository` do usuário guarda o estado do post
  numa tabela `posts`, e os eventos vão por `commit_in` na mesma
  transação. O `EventSourcedPostRepository` continua como o outro adaptador
  da mesma porta, na montagem em memória: o repositório é do usuário e
  troca sem tocar a aplicação.
- **Leitura:** `post_summaries` como entidade SeaORM, escrita pela projeção
  com `SeaOrm::new(context.transaction())`, no commit do token (uma vez
  por evento), e lida pelas queries com uma `DatabaseConnection` criada do
  mesmo `PgPool`.
- **Schema:** as migrations do blog (`sea-orm-migration`) incluem
  `cqrs_postgres::MIGRATIONS`.
- **Montagem:** `start()` passa a usar PostgreSQL; `in_memory()` continua
  para os testes sem banco.

Pronto quando:

- o servidor do blog roda sobre PostgreSQL com SeaORM;
- um teste mostra estado e eventos fazendo commit e sendo desfeitos
  juntos;
- um teste mostra a projeção SeaORM aplicando cada evento uma vez, mesmo
  com falha depois da escrita;
- `cargo tree -p cqrs-async` não tem `sqlx`, `sea-orm` nem `serde` com as
  features padrão do core;
- o portão passa.

## Fase 7b — Namespaces, unidade de trabalho do comando, posições

Desenhada em 2026-11-24, depois de ler como o Axon 5 faz (event handlers e
namespaces; event storage nos engines JPA, PostgreSQL e Axon Server). A
ordem vai da API que o usuário escreve para o motor por baixo; cada bloco
entra testado.

### 7b.1 A API final

**1a. Namespaces (D13).**

```rust
#[cqrs::projection(name = "post", events = PostEvent, namespaces("summaries", "notifications"))]
pub struct PostProjection<S, N> { summaries: Arc<S>, notifier: Arc<N> }

#[cqrs::event_handler(namespace = "summaries")]
impl<S: PostSummaryStore<Tx>, N, Tx: Send> EventHandler for PostProjection<S, N> { /* handle, reset */ }

#[cqrs::event_handler(namespace = "notifications")]
impl<S, N: Notifier, Tx: Send> EventHandler for PostProjection<S, N> { const REPLAY: bool = false; /* handle */ }
```

- `event_handler` implementa um trait interno por namespace, identificado
  por um ID constante (FNV do texto, em `const fn`; `&str` como parâmetro
  de tipo não existe no Rust estável), e gera uma asserção constante: o
  namespace tem de estar na lista da projeção.
- `projection` gera a rota (um `match` por índice de namespace) com
  `where Self:` um handler por namespace listado: faltando um, o erro diz
  qual (`#[diagnostic::on_unimplemented]`).
- O token de cada namespace é `"{projeção}.{namespace}"`: duas raízes
  nunca dividem token.
- Por baixo, ainda um `TrackingProcessor` por namespace (o motor muda em
  7b.2).
- `.policy(..)` sai; o blog migra; o guia passa a se chamar "Live Queries
  and Subscriptions" (retornos em stream de vários valores, como SSE e live
  queries, e eventos em tempo real, como GraphQL subscriptions e
  WebSockets).

**1b. Unidade de trabalho do comando (D7).**

```rust
#[cqrs::command_handler]
impl<R: PostRepository<PgTransaction>> CommandHandler for PublishPostHandler<R> {
    async fn handle(&self, command: &PublishPost, events: &mut Appender<PostEvent>, tx: &mut PgTransaction)
        -> Result<(), BlogError>
    {
        let post = self.posts.find(command.id, tx).await?.ok_or(NotFound(command.id))?;
        post.publish(events)?;
        let post = events.record(post);        // puro: o post depois dos eventos, na versão nova
        self.posts.save(&post, tx).await?;     // a porta de persistência recebe só a transação
        Ok(())
    }
}
```

- O gateway abre a transação, roda o handler, grava os eventos pendentes na
  mesma transação (o último passo, então os locks do DCB duram só até o
  `COMMIT`), faz o commit e avisa os processors. `Err` ou conflito desfaz;
  conflito roda o handler de novo.
- Sem `tx` na assinatura (ou `&mut ()`), o caminho é o de hoje: um append
  de um statement, sem `BEGIN`/`COMMIT`. Decidido em compile time.
- Saem `commit_in`, `Committed`, `announce()` e `PublishIn`/`Announce`.
- D15 e D16 entram aqui.

**1c. `cqrs-async-sea-orm` (D9).** A vista sai do exemplo para o crate.

Pronto quando: o blog roda nas montagens em memória e no PostgreSQL com a
API nova, os testes de aceitação do PostgreSQL passam (estado e eventos
juntos, projeção aplicando cada evento uma vez, subscriptions entre
instâncias), e o portão não cai no caminho sem transação.

**Como ficou (1a–1c).**

- 1a: `Projection` (nome, eventos, namespaces), `Route<Tx>` (gerado:
  um `match` por índice), `NamespaceHandler<ID, E, Tx>` e `HasNamespace<ID>`
  (escondidos, escritos pelas macros). `Events::projection(p)` devolve
  `ProjectionProcessors` (`with_options`, `reset`, `spawn`) e um
  `ProjectionHandle`; o `InlinePublisher` roda uma projeção com
  `with_projection`; `ProcessingContext::processor()` diz em que processor
  o evento está (`"post.summaries"`), o que deixa um interceptor da
  projeção inteira distinguir os namespaces. `#[cqrs::event_handler]` sem
  namespace preenche um `EventHandler` avulso (`events.processor(nome,
  handler)`).
- 1b: `CommandHandler` ganhou `type Unit` (`Autocommit` ou
  `InTransaction<Tx>`, pela assinatura) e `handle(.., events, tx)`;
  `Appender::record(entity)` substitui o `commit`; o gateway tem um campo
  `#[gateway(unit)]` com o `UnitOfWork<E, Mode>`; `append_writes` aplica a
  regra "os eventos da própria execução não reprovam a condição das
  gravações seguintes". Os dois modos não podiam ser dois impls chaveados
  por `()` e `M::Tx` (o compilador não prova que `M::Tx != ()`): por isso
  o modo é um tipo. `EventPublisher` ficou para appends fora de comandos
  (importação, testes).

### 7b.2 O motor dos namespaces

**Medido antes de reescrever o processor (2026-11-24, teste ignorado
`cost_per_namespace`):** 5000 eventos, handlers vazios, um processor por
namespace como hoje. 1 namespace: 2,6 s, ~5 mil transações, ~145 mil linhas
lidas; 4 namespaces: 4,6 s, ~18 mil transações, ~480 mil linhas. O custo
cresce com as **transações** (uma por evento por namespace: o trabalho do
handler, que faz commit com o token), não com a leitura do log (alguns
milhares de linhas por namespace, ~6% do total). Com lotes de 64
(`batch_size`), os mesmos 4 namespaces levaram 0,25 s.

**Decisão:** o leitor compartilhado e os claims por projeção ficam
adiados (ganho pequeno medido, refatoração grande do processor). A
alavanca é o lote, e ela ficou com o namespace: `batch = N` no
`#[cqrs::event_handler(namespace = ..)]` (`c75b727`). **O padrão continua
1, como no Axon** (decidido em 2026-11-24): um lote que falha é refeito
evento a evento, o que repetiria um efeito colateral, então o lote maior é
escolha explícita de quem sabe que tudo o que o handler faz é desfeito
com a transação. O desligamento de namespace lento
já vale hoje: cada namespace tem o seu processor.

O que estava previsto, para quando a medição justificar:

- Um processor por projeção e instância: uma leitura do log, uma fila e um
  worker por (namespace, segmento), cada um com o seu token e a sua
  transação.
- Um namespace lento é desligado do leitor compartilhado e lê sozinho (a
  regra que já existe para segmentos).
- Status por namespace; subscriptions esperam só os namespaces com
  `REPLAY = true`.
- Claims por projeção: uma query renova todos os (namespace, segmento) da
  instância.
- Opcional: o namespace declara as variantes que trata e o filtro vai ao
  SQL (`event_type = ANY(..)`), como o Axon com os tipos que os handlers
  suportam.
- Benchmark novo "N namespaces por projeção": leituras do log, eventos/s,
  consultas de claim/s.

### 7b.3 Posições e infraestrutura do PostgreSQL

- **3a. Estratégia A (D14), feita.** Medida antes: 16 escritores (~9 mil
  eventos/s) em dois logs deixavam o cursor de um processor com ~125 gaps
  em média (até 277) e 66 a 190 gravados no token; num log só, nenhum. A
  leitura do PostgreSQL passou a dizer, no mesmo snapshot, quais posições
  puladas nenhum log gravou (`LogPage::missing`), e o cursor só faz gaps
  delas (`Cursor::advance_over`). Depois, a mesma carga em dois logs:
  nenhum gap. A medição é o teste ignorado
  `gaps_from_other_logs_under_load`.
- **3b. Helper:** `cqrs_postgres::Postgres::new(pool)` com
  `pg.events::<E>(log)`, `pg.migrations()` e `pg.migrate()`, atrás de uma
  feature `tokio` ligada por padrão.
- **3c. Spike da B — adiado (2026-11-24).** A estratégia A zerou os gaps
  medidos com dois logs sob carga, e o portão passa inteiro sem ela; a B é
  uma refatoração de risco alto no centro do store. Volta se uma medição
  pedir. A latência entre instâncias (hoje por polling) vira um item
  separado e pequeno: `LISTEN/NOTIFY` no PostgreSQL, depois da Fase 8 e
  antes da release, sem mudar contratos. O que estava previsto para a B: IDs temporários e finalizador logo depois do commit
  do framework (mais uma varredura de segurança), `pg_notify` para as
  outras instâncias, DCB contando linhas não finalizadas; duas variantes
  (`UPDATE` ou tabela de posições só de acréscimo). Decide por A/B contra a
  3a. Referência: `PostgresqlEventStorageEngine` e `PostgresqlFinalizer` em
  `AxonIQ/axoniq-framework` (licença comercial: só o padrão, nada de
  código).
- **3d. Agrupador de unidades:** `(posts, users).stop()` / `.settled()`.

### 7b.4 Pendências abertas

- ~~Portão: A/B contra `5437e6e` numa máquina parada; perfil do p99 de 38 ms
  do open account~~ **Feito (`263f8cd`):** atual e base empatam em todas as
  medidas; o mixed p99 erra o limite nas duas (0,80 e 0,81), então o limite
  de 0,76 não se reproduz nesta máquina nem com a base. O p99 de 38 ms não
  voltou (15–26 ms). No micro, a unidade de trabalho do comando (7b.1b)
  custa ~15 ns por comando (14 → 29 ns); a D17 e a D18 não mudaram nada.
- Cenários novos no versus (repositório com estado, subscriptions entre
  instâncias): em aberto, sem bloquear a release.
- ~~O teste instável `racing_writers...`~~ **Feito (`984145d`).**
- O Tokio dentro do processor (7.5 parcial): **mantido e documentado** em
  event processing.

### 7b.5 Documentação

**Feito (2026-11-24, depois da Fase 8):** todas as páginas do site reescritas contra a API final. Pendências achadas
na revisão, resolvidas depois: dead letters nos namespaces de uma projeção (`7b5b78e`); o README com o MSRV por
crate (2026-11-25). Alguns trechos das páginas são esboços que usam só APIs reais mas não são compilados: os
completos são o quickstart (doctest) e os dois exemplos.

Depois de 7b.1 a 7b.3, a revisão inteira no site Docusaurus: unit of work,
namespaces, Live Queries and Subscriptions, storage e posições, SeaORM,
montagem à mão antes do `Cqrs` (7.6), quickstart, e a decisão de modelagem
"consistência forte só dentro de um log".

| Bloco | Depende de | Risco | Portão |
|---|---|---|---|
| 1a namespaces (API) | — | baixo | não |
| 1b unidade de trabalho | — | médio | sim (caminho sem transação) |
| 1c crate SeaORM | 1b | baixo | não |
| 2 motor dos namespaces | 1a | médio | sim |
| 3a posições A + 3b helper | — | baixo | sim |
| 3c spike B | 1b, 3a | alto | decide por ele |
| 3d agrupador | 1a | baixo | não |
| 4 pendências | — | baixo | é o portão |
| 5 docs | 1–3 | baixo | — |

## Fase 8 — API pública idiomática

Roteiro: rodar `cargo public-api` no começo, revisar a superfície inteira
contra as Rust API Guidelines, e guardar o resultado como snapshot no CI.

**8.1 Organização.** A raiz de `cqrs` reexporta cerca de 90 itens em uma
lista plana. Agrupar em módulos (`aggregate`, `command`, `query`, `event`,
`store`, `processing`, `subscription`, `testing`) e manter um `prelude`
pequeno.

**8.2 Nomes.**

| Hoje | Problema | Proposta |
|---|---|---|
| ~~`Appender` (struct) e `EventAppender` (trait)~~ | ~~Dois nomes quase iguais.~~ | Feito (D20): `CommandContext`; `record` mantido. |
| ~~`Projection`, `#[cqrs::projection]`~~ | ~~O blog anota uma policy de notificação como "projection".~~ | Mantido (D20): desde D12/D13 o efeito colateral é um namespace da projeção. |
| ~~`Handler` + `#[query_handler]`~~ | ~~Comandos têm `CommandHandler`, queries usam o genérico.~~ | Feito em D17: `QueryHandler<Q>`. |
| ~~`P`~~ | ~~É processor em `InlinePublisher` e publisher em `Cqrs`.~~ | Feito: `H` nos handlers do `InlinePublisher`. |

**8.3 Ids tipados.** O envelope só carrega `StreamId` como texto. O blog
converte na mão (`event.stream.id().parse()`, em
[post_summary.rs](../examples/blog/src/application/projections/post_summary.rs))
e compara strings em filtros. Oferecer `envelope.aggregate_id::<A>()` para
ids que implementam `FromStr`.

**8.4 Checklist de tipos públicos.**

- `#[non_exhaustive]` em enums de erro e de resultado (`StoreError`,
  `ResetError`, `TokenUpdate`).
- Structs de opções (`TrackingOptions`, `SubscriptionOptions`) com campos
  públicos impedem adicionar um campo sem quebrar: trocar por setters.
- `Debug` em todos os tipos públicos; faltam, entre outros, em `Loaded`,
  `Events`, `TrackingProcessor`, `ProcessorHandle` e `Cqrs`.
- `std::error::Error` em `ProcessingFailure`; exemplos da documentação não
  devem usar `()` como erro.
- `#[must_use]` em builders, em `ProcessorHandle` e no valor de
  `commit_in`.
- Traits que o usuário não deve implementar: `EventChain` selado;
  `DispatchVia`, que o derive implementa no crate do usuário e por isso não
  pode ser selado, com `#[doc(hidden)]`.
- Mutex envenenado: há 33 `expect("... poisoned")`. Uma biblioteca não deve
  derrubar todas as threads por um pânico em uma. Decidir entre recuperar o
  guard ou usar um mutex sem envenenamento.
- `cqrs-postgres` tem 23 conversões com `as` entre inteiros: trocar por
  `try_from` com erro.

**8.5 Custos por evento que a API fixa.** `EventEnvelope::event_type` é
`String` (uma alocação por evento, embora o tipo venha de `&'static str`)
e `Metadata` é um `BTreeMap` clonado para cada evento do commit. `Cow` e
`Arc` resolvem; mudar antes da primeira release, porque depois quebra.

**8.6 Macros.** Para cada macro, a documentação mostra o código
equivalente escrito à mão. Mensagens de erro revisadas com `trybuild`.

**8.7 Documentação.**

- Os blocos ` ```text ` e ` ```ignore ` do rustdoc viram doctests
  compilados.
- O rustdoc cita Axon ou NestJS dezenas de vezes. O mapa de conceitos fica
  em [architecture.md](architecture.md); cada item passa a se explicar
  sozinho.
- Tirar "PoC" das descrições e a referência a `cqrs_study.md`, que não
  existe ([lib.rs](../crates/cqrs/src/lib.rs)).
- Quickstart na documentação do crate e no README, montado à mão (7.6).
- Um exemplo mínimo em um arquivo só, além do blog em camadas.

Pronto quando: o snapshot de `cargo public-api` foi revisado item a item e
`cargo doc` não tem avisos.

## Fase 9 — Empacotamento e release

**Estado (2026-11-25):** feito tudo o que não depende de publicar, inclusive o `repository`
(github.com/Manuel-Antunes/cqrs-async), o README com a instalação e o MSRV por crate, e o CHANGELOG. Ficam a
publicação em si e o `cargo semver-checks` depois dela (ver "O que fica").

- Renomear os crates (D1): `cqrs` → `cqrs-async`, `cqrs-derive` →
  `cqrs-async-macros` (tem macros de atributo, não só derives),
  `cqrs-postgres` → `cqrs-async-postgres`; e, se a 7.10 sair do exemplo,
  `cqrs-async-sea-orm`. `publish = true`; versão `0.1.0-alpha.1`.
- O que o nome novo toca no código: `krate()` em
  [cqrs-derive/src/lib.rs](../crates/cqrs-derive/src/lib.rs) procura a
  dependência `cqrs` e cai em `::cqrs`; passa a procurar `cqrs-async` e
  emitir `::cqrs_async`. O `extern crate self as cqrs` de
  [lib.rs](../crates/cqrs/src/lib.rs) acompanha. Os atributos passam a ser
  escritos `#[cqrs_async::...]`, a menos que o usuário renomeie a
  dependência para `cqrs` no `Cargo.toml`, o que a busca por nome já
  suporta.
- Publicar o alpha cedo: é o que reserva os nomes.
- Metadados: `repository`, `readme`, `keywords`, `categories`,
  `[package.metadata.docs.rs]` com todas as features.
- `include` de `cqrs-postgres` contendo `.sqlx` e `migrations`.
- `cargo publish --dry-run` em cada um, na ordem macros → core →
  adaptadores, e um projeto de fora que dependa do pacote gerado e compile
  sem banco.
- CI: `cargo semver-checks` a partir da primeira versão publicada;
  `cargo deny`; o job de MSRV cobre cada crate com o seu MSRV.
- Rodar o portão uma última vez, com a máquina parada, e atualizar
  [performance.md](performance.md) com os números da versão publicada.

## Fase 10 — Mensageria: as features do Watermill nos contratos do Axon 5

Pedido em 2026-11-24: integrações com outros pub/subs (o AMQP do Watermill
é o exemplo) e as features fan-in, fan-out, mensagens com atraso e requeue
depois de erro, desenhadas com os contratos dos equivalentes do Axon 5.

Pesquisa feita no mesmo dia (documentação do Axon 5.2–5.4 e o código na
tag `axon-5.4.2`) mostra que boa parte desses equivalentes **não existe no
Axon Framework 5 gratuito**:

| Watermill | Equivalente no Axon 5 | Situação no Axon 5 |
|---|---|---|
| AMQP / Kafka (pub/sub) | `SubscribableEventSource.subscribe(..)` (entrada), `EventSink.publish(ctx, events)` (saída) | Os contratos existem; as extensões AMQP e Kafka ainda não têm versão 5 (as receitas de migração estão vazias). |
| Fan-out | Vários event processors sobre a mesma fonte, cada um com seus tokens | Existe; aqui já é assim (vários `TrackingProcessor` sobre o log). |
| Fan-in | `MultiStreamableMessageSource` (Axon 4) | Não existe no 5; o processor aceita uma `StreamableEventSource`. |
| Mensagens com atraso | `EventScheduler`, `DeadlineManager` | Saíram do 5 gratuito (só no módulo pago `axoniq-legacy`); a recomendação deles é um agendador externo (JobRunr, Quartz, db-scheduler) que chama o `CommandGateway`. |
| Requeue depois de erro | `SequencedDeadLetterQueue`, `EnqueuePolicy.decide(letter, cause)`, `Decisions.enqueue/requeue/evict/ignore`, `SequencedDeadLetterProcessor.processAny()` | Recurso pago do Axoniq Framework (volta na 5.1); no gratuito, `ErrorHandler` e backoff do segmento. |

Proposta: os contratos são nossos, com a forma do Axon onde ela existe.

- **10.1 Portas de broker:** `Publish` (já existe, 7.9) ganha o par de
  entrada na forma do `SubscribableEventSource`: um `Subscribe` que entrega
  lotes a um handler, com ack depois do handler. Adaptador AMQP (lapin) em
  crate próprio, com os presets do Watermill (fanout, queue, topic) como
  construtores.
  **Feito:** `cqrs::processing::{Subscribe, Consume, Incoming}` (o
  `Subscribe` roda até o `shutdown`, como `deliver_scheduled`; ack do lote
  inteiro depois do `Ok`, nack com requeue no erro, com pausa crescente
  entre falhas seguidas; `Incoming::decode(&codec)`). Crate
  `cqrs-async-amqp` (`cqrs_amqp`): `Amqp` (conexão compartilhada, um canal
  por publisher e por subscribe), `AmqpPublisher::{fanout, queue, topic}`
  (confirms, persistente, tipo/versão/posição/stream/metadados em
  propriedades e headers, declaração preguiçosa por tópico) e
  `AmqpSubscriber::{fanout, queue, topic}` (declara fila e binding na
  construção; `prefetch` é o maior lote; `dead_letter_failures()` rejeita
  sem requeue, para a DLX). Sem TLS por padrão (features `rustls`,
  `native-tls`); MSRV 1.89. Testes contra RabbitMQ (`CQRS_AMQP_URL`, no CI
  como serviço), incluindo o critério de pronto: o log como outbox para o
  RabbitMQ, consumido de volta como comandos num gateway, com entrega
  duplicada absorvida.
- **10.2 Fan-out:** já é o modelo; documentar, e um `Forward` por destino.
  **Feito:** documentado no padrão "The log as an outbox" (um `Forward` por
  destino, cada um com seu token; ou um forwarder e uma fila por serviço
  num exchange fanout), com teste: um broker fora do ar não segura o outro,
  e os dois recebem tudo, em ordem.
- **10.3 Fan-in:** com a ordem global mantida (D14), uma projeção sobre
  vários logs é um cursor só com `log IN (..)`
  (`#[cqrs::projection(events = (PostEvent, UserEvent))]`). O texto abaixo
  era a alternativa antes dessa decisão: uma fonte que junta vários logs com um cursor por fonte
  (sem ordem global entre fontes), para um processor; ou, como o Watermill,
  republicar num log só (perde a ordem original). Decidir pelo caso de uso.
  **Feito (opção B, escolhida em 2026-11-25):** o lado de leitura nomeia o
  que junta, num enum com uma variante por tipo de evento de log,
  `#[derive(DomainEvent)] #[event(fan_in)]` (tipo, versão e tags delegados
  ao evento embrulhado; `From` de cada um). A projeção continua com um
  tipo de evento só, então namespaces, batch, replay, `updated` e
  interceptors não mudam. Em memória: stores que compartilham a numeração
  (`InMemoryEventStore::beside`), lidos por `FanIn` (lê a última posição
  de todos antes, então nada é pulado) e `Events::fan_in((&posts,
  &users))`. No PostgreSQL: `PostgresFanIn` (uma faixa do índice `(log,
  position)` por log via `LATERAL`, juntadas; gaps pelo mesmo cursor) e
  `Postgres::fan_in((&posts, &users))`, cada log decodificado pelo seu
  store, com upcasters. O fan-in segue as fontes (`Events::follow`): os
  commits delas, e as notificações do `listen`, acordam os processors.
  Testes em memória e no PostgreSQL (ordem global, um terceiro log no meio
  que não vira gap, processor acordado pelos dois logs, token próprio).
- **10.4 Atraso:** uma porta `Schedule` (comando ou evento, com hora) sobre
  uma tabela persistida e lida por hora; o disparo chama o gateway e a
  decisão confere o estado de novo. Nada em memória: o `SimpleEventScheduler`
  do Axon perdia tudo no shutdown.
- **10.5 Requeue:** a fila de dead letters ganha a política do Axon:
  `EnqueuePolicy` (`decide(letter, cause) -> Decision`, com `Enqueue`,
  `Requeue`, `Evict`, `Ignore`) e diagnósticos/`last_touched` no letter,
  para retries que valem entre instâncias. Requeue "para o fim da fila" só
  com a política de concorrência total: por agregado ele quebra a ordem.

Pronto quando: um exemplo publica no RabbitMQ pelo log como outbox e
consome de volta mandando comandos ao gateway, com um teste de cada
feature.

## Riscos

- **A Fase 7 muda contratos que todo adaptador implementa** (token store,
  event store, publisher). Como nada foi publicado, é o momento; o blog, os
  benchmarks e os testes de PostgreSQL são os usuários a migrar e medem o
  esforço.
- **7.2 e 5.2 são desenho novo, não limpeza.** As duas começam por um spike
  com o teste que define o pronto, antes de mexer na API. Na 7.2, os pontos
  abertos são o `announce()` esquecido com o publisher inline e os locks do
  DCB presos na transação do usuário.
- **7.4 toca o caminho quente do append e da leitura do log.** Portão de
  benchmark no commit que fizer a divisão.
- **SeaORM 2 pede Rust 1.94.** Fica fora do MSRV dos crates principais
  enquanto morar em crate próprio ou no exemplo.
- **O portão depende da máquina.** Comparar sempre na mesma máquina, com a
  máquina parada ou em A/B alternado, nunca com números de outro ambiente.

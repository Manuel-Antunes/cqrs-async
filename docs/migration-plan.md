# Plano de migração: de PoC para biblioteca

Estado em 2026-11-23. Este plano leva o workspace (`cqrs`, `cqrs-derive`,
`cqrs-postgres`) de prova de conceito a uma biblioteca publicável: corrige
os problemas encontrados na revisão, remove o que está duplicado e revisa a
API pública para que seja idiomática.

Ele assume que quem executa não leu a revisão: cada item diz o que está
errado, onde, e o que fazer.

## Princípios

1. **Compile-time primeiro.** Roteamento de mensagens, cadeias de
   interceptors e grupos de handlers continuam estáticos: é onde um erro de
   compilação vale mais e onde o custo medido é ~4 ns por comando.
2. **`dyn` é permitido no encanamento**, quando remove parâmetros de tipo
   ou impls de repasse, e só se o portão de benchmark abaixo passar. Cada
   troca para `dyn` entra em um commit próprio, para poder ser revertida
   sozinha.
3. **Um jeito de fazer cada coisa.** Onde hoje há dois ou três caminhos
   (escrita, dispatch, subscriptions), fica um.
4. **Teste antes da correção.** Todo bug da Fase 1 começa por um teste que
   falha.

## Portão de benchmark

A regra é não cair em relação à biblioteca concorrente (cqrs-es 0.5 com
postgres-es 0.5). O portão roda ao fim das Fases 2, 3, 5 e 6, e depois de
cada commit que introduz `dyn`.

```text
CQRS_POSTGRES_URL=postgres://... cargo run --release -p cqrs-versus -- 3
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

Os limites são a base menos os ~20% de ruído que o próprio documento de
performance declara, e nunca cruzam 1,0× contra nós. Os micro-benchmarks
(gateway 4,0 ns; quatro interceptors 15,6 ns; evento na projeção 18,5 ns)
são informativos: uma regressão ali precisa de explicação no commit, mas
quem decide é a tabela acima.

A base precisa ser regravada na Fase 0, na máquina em que a migração vai
rodar: `benchmarks/results/` está no `.gitignore`, então os números de
hoje só existem no disco de quem os mediu.

## Decisões a confirmar

| # | Decisão | Recomendação | Bloqueia |
|---|---|---|---|
| D1 | Nome dos crates. `cqrs`, `cqrs-core` e `cqrs-postgres` já existem no crates.io. | **Decidido: `cqrs-async`**, com `cqrs-async-macros` e `cqrs-async-postgres`. Os três estavam livres em 2026-11-23 (`cargo info`); só ficam garantidos na primeira publicação. | Fase 8 |
| D2 | Vários enums de evento no mesmo banco. | Log nomeado por store (item 1.2, opção B). | 1.2 |
| D3 | O que acontece quando um handler inline falha. | Política explícita no publisher inline; padrão: registrar o erro. | 1.1 |
| D4 | Mecanismo único de subscription. | Emitters (o que entendi da sua preferência por "subscriptions por meio de implementações"). `LiveQueries` sai. | Fase 5 |
| D5 | Manter o modo sem `tokio`. | Manter só se couber em um módulo, sem `cfg` espalhado, e com um job de CI que o compile. | 6.6 |
| D6 | Publisher no construtor do handler (Fase 3). | Fazer: é a mudança mais visível para quem usa, mas remove cinco itens públicos e a reescrita de assinatura pela macro. | Fase 3 |

## Visão geral

| Fase | O que entrega | Tamanho | Depende de |
|---|---|---|---|
| 0 | Base: commit inicial, formatação, CI, benchmark de referência | P | — |
| 1 | Os quatro problemas corrigidos | M | 0, D2, D3 |
| 2 | Um caminho de escrita | M | 1 |
| 3 | Publisher no handler; sai a torre `DispatchIn` | G | 2, D6 |
| 4 | Um mecanismo de dispatch | P | 3 |
| 5 | Um mecanismo de subscription, correto com várias instâncias | G | 1, D4 |
| 6 | Encanamento: stores, tokens, grupos, `dyn` onde paga | M | 3 |
| 7 | API pública idiomática | G | 2–6 |
| 8 | Empacotamento e primeira release | P | 7, D1 |

As Fases 4 e 5 não dependem uma da outra e podem andar em paralelo com a 6.

---

## Fase 0 — Base

Nada aqui muda comportamento.

- **0.1 Commit inicial.** O repositório não tem nenhum commit: tudo está
  apenas em stage. Sem histórico não há como revisar nem reverter as fases
  seguintes.
- **0.2 Formatação.** `cargo fmt --all --check` acusa 601 trechos. Adicionar
  um `rustfmt.toml` (decidir a largura: hoje há linhas de 150+ colunas) e
  formatar tudo em um commit só, separado de qualquer mudança de código.
- **0.3 CI.** Não existe. Jobs: `fmt --check`; `clippy --workspace
  --all-targets -D warnings`; `test --workspace` com um serviço PostgreSQL
  (os testes de `cqrs-postgres` hoje são pulados sem banco);
  `test --doc`; `doc` com `RUSTDOCFLAGS=-D warnings`; matriz de features
  (`--no-default-features`, cada feature sozinha); MSRV 1.89.
- **0.4 Cache do sqlx.** `.sqlx` está no `.gitignore` e não é rastreado.
  Como todo SQL é `sqlx::query!`, um clone limpo não compila
  `cqrs-postgres` sem um banco migrado, e um pacote publicado também não
  (o `cargo package` respeita o `.gitignore`). Rastrear
  `crates/cqrs-postgres/.sqlx` e adicionar `cargo sqlx prepare --check` ao
  CI.
- **0.5 Benchmark de referência.** Rodar o portão, gravar o resultado em um
  arquivo rastreado e atualizar a tabela deste plano se a máquina for
  outra.
- **0.6 Arquivos de projeto.** `README.md`, `LICENSE` (o manifesto diz MIT,
  o arquivo não existe), `CHANGELOG.md`.

Pronto quando: CI verde em um commit que só contém formatação e
configuração. Os testes do core já passam hoje (15 binários de teste, sem
falhas).

## Fase 1 — Correções

### 1.1 Falha de handler inline some no caminho do gateway

`publish` descarta o resultado do processamento com `let _ =`
([event_sourced.rs:575](../crates/cqrs/src/event_sourced.rs#L575)). Pelo
gateway, uma projeção inline que falha não deixa rastro. A documentação
([architecture.md:131](architecture.md#L131)) promete `Saved::processing`,
que só existe na API antiga; o teste que cobre isso
([flow.rs:146](../examples/blog/tests/flow.rs#L146)) usa essa API antiga.

- Teste: comando pelo gateway de `in_memory()`, com o notifier fora do ar;
  a falha precisa ser observável.
- Correção (D3): o publisher inline recebe uma política de falha
  (`on_failure`), com padrão de registrar o erro. Devolver `Err` ao
  chamador seria enganoso: o evento já está gravado.
- A forma final sai da Fase 2, que cria o publisher inline como tipo
  próprio; aqui basta parar de descartar.

### 1.2 Dois enums de evento no mesmo PostgreSQL

O guia pede um enum por agregado
([01-domain.md:50](guide/01-domain.md#L50)), mas `read_all` e `read` leem
`cqrs_events` inteira
([event_store.rs:206](../crates/cqrs-postgres/src/event_store.rs#L206),
[:227](../crates/cqrs-postgres/src/event_store.rs#L227)). Um
`PostgresEventStore<PostEvent>` vai encontrar eventos de outro enum, falhar
ao decodificar e repetir do mesmo cursor para sempre. Isso foi lido no
código, não reproduzido: nenhum teste usa dois enums no mesmo schema.

- Teste: dois enums, um schema, um processor para cada; ambos precisam
  avançar.
- Opção A — um enum por log: o enum da aplicação agrupa os enums dos
  agregados. É coerente com o DCB (o teste já usa um `SchoolEvent` para
  curso e aluno), mas muda `Aggregate::Event`, `Appender` e todo handler.
- Opção B (recomendada) — log nomeado: `PostgresEventStore::new(pool,
  "posts")`, coluna `log` em `cqrs_events` e em `cqrs_event_tags`, índice
  `(log, position)`, e todas as leituras globais filtradas por ele. Não
  muda o modelo de domínio.
- Risco da B a medir no spike: com uma sequência de posições global, as
  posições dos outros logs aparecem como gaps no cursor até o horizonte
  passar. Se isso inflar os tokens sob carga, usar uma sequência por log.
- Corrigir o guia para dizer o que vale depois da decisão.

### 1.3 Dead letters

Dois defeitos e uma lacuna.

- Em [processor.rs:736](../crates/cqrs/src/streaming/processor.rs#L736), um
  erro ao consultar a fila vira "não está parqueado"
  (`unwrap_or(false)`), e o evento é tratado à frente dos anteriores
  parqueados do mesmo agregado. Na linha 665 o mesmo erro vira `true`.
  Correção: erro na consulta entra no mesmo backoff do `park`.
- Só existe `InMemoryDeadLetters`. Com PostgreSQL o token avança, a carta
  fica na memória e se perde no restart: o evento nunca mais é tratado.
  Criar `PostgresDeadLetters` (tabela `cqrs_dead_letters`), com o `enqueue`
  na mesma transação que move o token. Isso pede que a fila participe da
  unidade de trabalho (`enqueue_in(&mut tx, ...)`).
- Testes: fila que falha na consulta não quebra a ordem; processo que morre
  depois de parquear ainda encontra a carta.

### 1.4 Snapshots fora do caminho recomendado

`cqrs::load` não usa snapshots
([event_sourced.rs:520](../crates/cqrs/src/event_sourced.rs#L520)) e o
commit pelo publisher nunca tira um. Só funciona quando o adapter do
repositório guarda um `EventSourced` e chama `.load`, como o benchmark faz
e o blog não.

- Teste: repositório montado como o guia ensina, com snapshots a cada N;
  depois de N eventos a carga lê só a cauda.
- Correção: sai da Fase 2 (o carregador com snapshots passa a ser o único
  jeito de carregar).

Pronto quando: os quatro testes novos passam e a documentação citada bate
com o comportamento.

## Fase 2 — Um caminho de escrita

`EventSourced` hoje faz três papéis: carrega (com snapshots), oferece uma
API antiga de escrita (`create`/`update` com closures, `append*` devolvendo
`Saved`) e implementa `EventPublisher`. Só o terceiro é o caminho
recomendado, e é o que não tira snapshot nem reporta falha.

- **2.1 Separar em dois tipos.** Um carregador por agregado
  (`load`, `load_in`, snapshots) e um publisher inline por tipo de evento
  (grava e roda os handlers na mesma chamada). `Events` continua sendo o
  publisher assíncrono.
- **2.2 Remover a API antiga.** `create`, `update`, `append`,
  `append_with`, `append_if`, `Saved`, `with_conflict_retries`,
  `Events::event_sourced` e `Appender::save_with` (sem nenhum chamador).
  `conflict_retries` fica só em `CommandHandler`.
- **2.3 Uma implementação de carga.** O ramo de critérios está copiado
  quatro vezes (linhas 302, 400, 525 e 551 de `event_sourced.rs`). As
  funções livres `load`/`load_in` viram chamadas ao carregador, ou saem.
- **2.4 Migrar os chamadores.** `tests/repository.rs`, `tests/snapshots.rs`,
  `examples/blog/tests/domain.rs`, `benchmarks/benches/micro.rs`,
  `benchmarks/versus/src/ours.rs`. Testes de regra de domínio passam para
  `AggregateFixture`; os de persistência, para o gateway.
- **2.5 Blog.** `EventSourcedPostRepository` passa a usar o carregador, e
  com isso ganha snapshots (fecha 1.4).

Pronto quando: existe um único caminho de carga e um único de commit, e o
portão passa. O replay com snapshot é a medida a observar.

## Fase 3 — Publisher no handler

Para o gateway entregar o publisher a cada execução, os comandos têm uma
hierarquia paralela à das queries: `DispatchIn`, `DispatchInVia`,
`Published` e `WithPublisher`, este implementado para todo `T`
([command_handler.rs:511](../crates/cqrs/src/command_handler.rs#L511)).
Além disso `#[command_handler]` reescreve a assinatura de `handle` para
inserir um genérico oculto
([handlers.rs:50](../crates/cqrs-derive/src/handlers.rs#L50)).

Forma proposta: o handler recebe o publisher no construtor, como já recebe
o repositório.

```rust
pub struct PublishPostHandler<R, P> { posts: Arc<R>, events: P }

#[cqrs::command_handler]
impl<R: PostRepository, P: EventPublisher<PostEvent>> CommandHandler for PublishPostHandler<R, P> {
    async fn handle(&self, command: &PublishPost, decision: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.publish(decision)?;
        decision.commit(post, &self.events).await?;
        Ok(())
    }
}
```

- **3.1** `Appender<E, P>` perde o segundo parâmetro; `commit` recebe o
  publisher. O controle de retry (`saved`, `lost_race`) continua nele.
- **3.2** Removidos: `DispatchIn`, `DispatchInVia`, `Published`,
  `WithPublisher`, e o ramo `commands` de `Gateway` que gera impls
  genéricas sobre `__X`. `#[gateway(commands)]` continua existindo só para
  gerar o laço de retry (`dispatch_command`).
- **3.3** `#[command_handler]` volta a apenas preencher `type Command` e
  `type Event`, sem tocar na assinatura.
- **3.4** `Cqrs`: o parâmetro `P` e `publisher()` saem. Como a unidade cria
  o `Events`, o builder entrega o publisher a quem monta o gateway:
  `.commands(|events| PostCommandGateway::new(posts, events))`. Isso também
  acaba com o `Events` guardado duas vezes
  ([unit.rs:99](../crates/cqrs/src/unit.rs#L99)).
- **3.5** Atualizar a documentação da macro, que ainda descreve um formato
  intermediário ([cqrs-derive/src/lib.rs:133](../crates/cqrs-derive/src/lib.rs#L133)).

Custo assumido: cada handler de comando ganha um parâmetro de tipo e um
argumento no construtor.

Pronto quando: os testes de compilação negativa (`compile_fail`) estão
reescritos para a forma nova e o portão passa. O micro-benchmark do gateway
é a medida a observar.

## Fase 4 — Um mecanismo de dispatch

- **4.1 Remover `Bus`.** A lista de tipos de
  [bus.rs](../crates/cqrs/src/bus.rs) só é usada em testes, um benchmark e
  na documentação, e expõe `Cons`, `Nil`, `Here`, `There` e `Route`.
  `#[derive(Gateway)]` cobre o caso com mensagens de erro melhores. Se o
  exemplo sem macros for importante, ele vira uma página do guia mostrando
  o `impl Dispatch` escrito à mão.
- **4.2 Uma forma de passar metadata.** Hoje cada nível tem o par com e
  sem: `handle`/`handle_with`, `dispatch`/`dispatch_with`,
  `send`/`send_with`. Deixar um método obrigatório por trait e o atalho só
  na ponta (`send`, `query`).

## Fase 5 — Um mecanismo de subscription

Hoje há dois: `LiveQueries` reexecuta a query quando um evento relevante
foi projetado; `SubscriptionQueries` recebe o que a projeção emite. Pela
decisão D4 ficam os emitters.

- **5.1 Remover `LiveQueries`, `RefreshedBy` e uma das duas funções
  `distinct`** ([live_queries.rs:216](../crates/cqrs/src/streaming/live_queries.rs#L216),
  [updates.rs:232](../crates/cqrs/src/streaming/updates.rs#L232)). No blog
  saem as impls de `RefreshedBy`, que hoje não são usadas por ninguém.
  `LocalProgress` e `GlobalProgress` moram no mesmo arquivo e ficam: 5.2
  precisa deles.
- **5.2 Fechar o buraco de várias instâncias.** Um emitter só dispara na
  instância que projetou o evento; um assinante conectado a outra nunca
  recebe a atualização. Era o motivo de `LiveQueries` existir. Proposta:
  a emissão deixa de ser feita pelo worker que detém o segmento e passa a
  ser feita, em toda instância, por uma tarefa que segue o feed de eventos,
  espera o progresso global da projeção (`GlobalProgress`, que já existe) e
  então chama o hook de emissão. Com uma instância só, usa o progresso
  local e o custo é o de hoje.
- **5.3 Teste que define o pronto:** duas instâncias sobre o mesmo
  PostgreSQL; assinante na instância B recebe a mudança projetada pela A.
- **5.4 Separar os dois usos de `committed`.** Hoje o mesmo hook serve a
  "efeito pós-commit na instância que processou" e a "emitir atualização".
  Com 5.2 eles têm chamadores diferentes: dar um nome a cada um.
- **5.5 Candidato a `dyn`:** o registro de emitters. Hoje é uma struct com
  `#[derive(QueryUpdates)]` e o trait `UpdatesOf<Q>`, que leva o parâmetro
  `U` até `Cqrs`. Um registro indexado pelo tipo da query removeria o
  derive e o parâmetro, ao custo de uma busca por emissão. Só entra se o
  cenário "live queries" do benchmark de carga não cair.

Pronto quando: 5.3 passa, o guia de live queries descreve um mecanismo só,
e o portão passa.

## Fase 6 — Encanamento

- **6.1 Stores como handles.** `InMemoryEventStore` e `InMemoryTokenStore`
  passam a ter o `Arc` por dentro e a ser `Clone`, como
  `PostgresEventStore` já é. Saem as impls de repasse para `Arc<T>` de
  `EventStore`, `TokenStore`, `TransactionalTokenStore`, `DeadLetterQueue`
  e `EventFeed`. Ficam as de `Handler`, `Dispatch` e `EventHandler`, que
  usuários de fato compartilham em `Arc`.
- **6.2 Um trait de token store.** Todo consumidor exige
  `TransactionalTokenStore`; a separação de `TokenStore` não se paga.
  Unir, com `type Transaction`. `store` e `store_in` viram um.
  `TokenUpdate::Untouched` nunca é devolvido por um store: vira detalhe
  interno do processor.
- **6.3 Grupos de handlers.** `handle_in` e `committed_in` são métodos
  públicos de `EventHandler` que o usuário nunca deve chamar nem
  implementar: mover para um trait interno, selado. E resolver a tupla com
  dois significados: `(a, b)` é "isolados" como `EventProcessor`
  ([event_sourced.rs:176](../crates/cqrs/src/event_sourced.rs#L176)) e
  "tudo ou nada" como `EventHandler`
  ([message.rs:276](../crates/cqrs/src/message.rs#L276)). Um significado
  para a tupla, um wrapper com nome para o outro.
- **6.4 Erros com causa (`dyn`).** `StoreError::Backend(String)` e
  `HandlerError(String)` jogam fora o erro original. Guardar
  `Box<dyn Error + Send + Sync>` e expor `source()`. `StoreError` deixa de
  ser `PartialEq`: os testes que comparam erros passam a usar `matches!`.
- **6.5 Dead letters por `dyn`: descartado.** A fila virou
  `DeadLetterQueue<E, Tx>`, com `Tx` sendo a transação do token store: a
  carta entra e sai da fila na unidade de trabalho do processor, e é o tipo
  que impede combinar uma fila em memória com tokens em banco. Apagar o
  tipo perderia essa checagem; o parâmetro `D` fica.
- **6.6 Feature `tokio` (D5).** São 12 `cfg` em `lib.rs` e `unit.rs` para
  um modo cujo único consumidor são testes. Mover o builder com processors
  para dentro do módulo `streaming`, de modo que não sobre `cfg` fora dele.
- **6.7 `TrackingProcessor::work`.** Manter o leitor compartilhado: o ganho
  está medido (com 14 segmentos, de 43 mil para até ~497 mil eventos/s em
  memória). Separar os dois modos da função
  ([processor.rs:394](../crates/cqrs/src/streaming/processor.rs#L394)),
  roteado e autônomo, em duas, e tirar a checagem de `REPLAY` duplicada
  entre `handle` e `handle_in`.
- **6.8 Schema.** `cqrs_head` e `cqrs_consistency` são criadas e nunca
  usadas. Como nada foi publicado, as três migrações (incluindo a conversão
  de `jsonb` para `json`) viram uma só, já na forma final e com as tabelas
  novas das Fases 1.2 e 1.3.
- **6.9 Sem chamadores.** `EventCriteria::matches` e `matches_event`.

O que fica estático de propósito: as macros de tupla para handlers,
interceptors e upcasters. É o que dá a cadeia de interceptors a ~3 ns por
elo.

## Fase 7 — API pública idiomática

Roteiro: rodar `cargo public-api` no começo, revisar a superfície inteira
contra as Rust API Guidelines, e guardar o resultado como snapshot no CI.

**7.1 Organização.** A raiz de `cqrs` reexporta cerca de 90 itens em uma
lista plana. Agrupar em módulos (`aggregate`, `command`, `query`, `event`,
`store`, `processing`, `subscription`, `testing`) e manter um `prelude`
pequeno.

**7.2 Nomes.**

| Hoje | Problema | Proposta |
|---|---|---|
| `Appender` (struct) e `EventAppender` (trait) | Dois nomes quase iguais para coisas diferentes: o contexto do handler e onde o domínio grava. | Renomear a struct (`Decision`, `CommandContext`). |
| `Projection`, `#[cqrs::projection]` | O blog anota uma policy de notificação como "projection". | Nome neutro para "handler com nome de processor"; `projection` e `policy` como apelidos, se fizer falta. |
| `Handler` + `#[query_handler]` | Comandos têm `CommandHandler`, queries usam o genérico. | `QueryHandler` simétrico. |
| `Loaded::new(Post::initial(&id))` | Criar um agregado novo pede duas chamadas e um trait importado, e a criação era decidida num `Post` vazio (dava para gravar um segundo `Created` num post existente). | **Feito:** criação é função associada (`Post::create(id, ..)`, como o handler estático do Axon 5) e o commit vai para `New::<Post>(id)`, que exige stream inexistente. `Loaded::new` saiu da API pública. |
| `EventSourced` | Deixa de existir como está na Fase 2. | Nomear os dois tipos pelo papel. |
| `P` | É processor em `EventSourced` e publisher em `Appender` e `Cqrs`. | Uma letra por papel. |

**7.3 Ids tipados.** O envelope só carrega `StreamId` como texto. O blog
converte na mão (`event.stream.id().parse()`,
[post_summary.rs:76](../examples/blog/src/application/projections/post_summary.rs#L76))
e compara strings em filtros. Oferecer `envelope.aggregate_id::<A>()` para
ids que implementam `FromStr`.

**7.4 Checklist de tipos públicos.**

- `#[non_exhaustive]` em enums de erro e de resultado (`StoreError`,
  `ResetError`, `TokenUpdate`): hoje não há nenhum no workspace.
- Structs de opções (`TrackingOptions`, `SubscriptionOptions`) com campos
  públicos impedem adicionar um campo sem quebrar: trocar por setters.
- `Debug` em todos os tipos públicos; faltam, entre outros, em `Loaded`,
  `Events`, `TrackingProcessor`, `ProcessorHandle` e `Cqrs`.
- `std::error::Error` em `ProcessingFailure`; exemplos da documentação não
  devem usar `()` como erro.
- `#[must_use]` em builders e em `ProcessorHandle`.
- Traits que o usuário não deve implementar: `EventChain` selado;
  `DispatchVia`, que o derive implementa no crate do usuário e por isso não
  pode ser selado, com `#[doc(hidden)]`.
- Mutex envenenado: há 33 `expect("... poisoned")`. Uma biblioteca não deve
  derrubar todas as threads por um pânico em uma. Decidir entre recuperar o
  guard ou usar um mutex sem envenenamento.
- `cqrs-postgres` tem 23 conversões com `as` entre inteiros: trocar por
  `try_from` com erro.

**7.5 Custos por evento que a API fixa.** `EventEnvelope::event_type` é
`String` (uma alocação por evento, embora o tipo venha de `&'static str`)
e `Metadata` é um `BTreeMap` clonado para cada evento do commit. `Cow` e
`Arc` resolvem; mudar antes da primeira release, porque depois quebra.

**7.6 Macros.** Para cada macro, a documentação mostra o código
equivalente escrito à mão. Mensagens de erro revisadas com `trybuild`.

**7.7 Documentação.**

- 17 blocos de exemplo no rustdoc são ` ```text ` ou ` ```ignore `:
  convertê-los em doctests compilados.
- O rustdoc cita Axon ou NestJS 54 vezes. O mapa de conceitos fica em
  [architecture.md](architecture.md); cada item passa a se explicar
  sozinho.
- Tirar "PoC" das descrições e a referência a `cqrs_study.md`, que não
  existe ([lib.rs:1](../crates/cqrs/src/lib.rs#L1)).
- Quickstart na documentação do crate e no README.
- Um exemplo mínimo em um arquivo só, além do blog em camadas.

**7.8 `cqrs-postgres`.**

- `PgPool` e `PgTransaction` estão na API pública: reexportar `sqlx` e
  documentar que uma troca de versão maior do sqlx é quebra.
- Expor as migrações também para quem as gerencia por conta própria, além
  de `migrate()`.
- Nomes de tabela são fixos (limitação do `query!`): documentar o uso de
  `search_path` por schema, que os testes já fazem.

Pronto quando: o snapshot de `cargo public-api` foi revisado item a item e
`cargo doc` não tem avisos.

## Fase 8 — Empacotamento e release

- Renomear os crates (D1): `cqrs` → `cqrs-async`, `cqrs-derive` →
  `cqrs-async-macros` (tem macros de atributo, não só derives),
  `cqrs-postgres` → `cqrs-async-postgres`. `publish = true`; versão
  `0.1.0-alpha.1`.
- O que o nome novo toca no código: `krate()` em
  [cqrs-derive/src/lib.rs:21](../crates/cqrs-derive/src/lib.rs#L21) procura
  a dependência `cqrs` e cai em `::cqrs`; passa a procurar `cqrs-async` e
  emitir `::cqrs_async`. O `extern crate self as cqrs` de
  [lib.rs:38](../crates/cqrs/src/lib.rs#L38) acompanha. Os atributos
  (`#[cqrs::projection]`) passam a ser escritos `#[cqrs_async::...]`, a
  menos que o usuário renomeie a dependência para `cqrs` no `Cargo.toml`,
  o que a busca por nome já suporta.
- Publicar o alpha cedo: é o que reserva os três nomes.
- Metadados: `repository`, `readme`, `keywords`, `categories`,
  `[package.metadata.docs.rs]` com todas as features.
- `include` de `cqrs-postgres` contendo `.sqlx` e `migrations`.
- `cargo publish --dry-run` nos três, na ordem macros → core → postgres, e
  um projeto de fora que dependa do pacote gerado e compile sem banco.
- CI: `cargo semver-checks` a partir da primeira versão publicada;
  `cargo deny`.
- Rodar o portão uma última vez e atualizar
  [performance.md](performance.md) com os números da versão publicada.

## Riscos

- **Fase 3 e Fase 7 quebram todo código que usa a biblioteca.** Como nada
  foi publicado, é o momento de fazer; o blog e os benchmarks são os
  usuários a migrar, e servem de medida do esforço.
- **1.2 e 5.2 são desenho novo, não limpeza.** As duas começam por um
  spike com o teste que define o pronto, antes de mexer na API.
- **O portão depende da máquina.** Comparar sempre com a base regravada na
  Fase 0 na mesma máquina, nunca com os números de outro ambiente.

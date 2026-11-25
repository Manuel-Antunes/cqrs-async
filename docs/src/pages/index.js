import React, { useState } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';

import styles from './index.module.css';

const HERO_CODE = `#[cqrs::command(error = BlogError)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RetitlePost {
    pub id: PostId,
    pub title: String,
}

#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<RetitlePost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &RetitlePost,
        ctx: &mut CommandContext<PostEvent>,
        tx: &mut R::Tx,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(command.id, tx).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), ctx)?;
        let post = ctx.record(post);       // the post after its events
        self.posts.save(&post, tx).await?; // same transaction: the framework commits
        Ok(())
    }
}`;

const TOUR = [
  {
    id: 'domain',
    tab: '1. Domain',
    title: 'Events are facts. Aggregates are plain structs.',
    text: (
      <>
        A decision reads <code>&amp;self</code> and appends events; it cannot assign state, so that
        does not compile. <code>evolve</code> is the only place state changes, when an event is
        recorded and when it is replayed.
      </>
    ),
    link: ['/guide/domain', 'Part 1: the domain'],
    code: `#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent)]
pub enum PostEvent {
    #[event(name = "post.created")]
    Created { id: PostId, title: String },
    #[event(name = "post.retitled")]
    Retitled { title: String },
    #[event(name = "post.published")]
    Published,
}

#[cqrs::aggregate(name = "post", event = PostEvent)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Post { id: PostId, title: String, published: bool }

impl Post {
    pub fn publish(&self, events: &mut impl EventAppender<PostEvent>)
        -> Result<(), PostError>
    {
        if self.published {
            return Err(PostError::AlreadyPublished);
        }
        events.append(PostEvent::Published);
        Ok(())
    }
}`,
  },
  {
    id: 'test',
    tab: '2. Test it',
    title: 'Given events, when a decision runs, expect events or an error.',
    text: <>No store, no database, no async. Domain rules are tested as data.</>,
    link: ['/guide/domain#testing-the-rules', 'Testing the rules'],
    code: `use cqrs::testing::AggregateFixture;

#[test]
fn a_post_is_published_once() {
    AggregateFixture::<Post>::new(ID)
        .given([created(), PostEvent::Published])
        .when(|post, events| post.publish(events))
        .expect_error(PostError::AlreadyPublished)
        .expect_no_events();
}

#[test]
fn creating_a_post_appends_created() {
    AggregateFixture::<Post>::creating(ID, |events| Post::create(ID, "  Hello ", events))
        .expect_events([created()])
        .expect_state(|post| assert_eq!(post.title(), "Hello"));
}`,
  },
  {
    id: 'gateway',
    tab: '3. Gateway',
    title: 'Routing the compiler can see.',
    text: (
      <>
        The gateway is the handler: one struct holding the repository, one impl per command it
        lists. A command listed without its impl does not compile, and neither does sending one
        the gateway does not handle. Callers depend on the commands they send, not on the gateway.
      </>
    ),
    link: ['/guide/commands#the-command-gateway', 'Part 2: commands'],
    code: `#[cqrs::command_gateway(
    commands = PostCommands,
    handles(CreatePost, RetitlePost, PublishPost)
)]
pub struct PostCommandGateway<R, P> {
    posts: R,              // your repository
    #[gateway(unit)]
    events: P,             // the unit of work every command runs in
}

// Code that sends commands asks only for the ones it sends:
pub async fn create_and_publish<G>(gateway: &G, title: &str)
    -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{
    let id = gateway.send(CreatePost { title: title.to_owned() }).await?;
    gateway.send(PublishPost { id }).await?;
    Ok(id)
}`,
  },
  {
    id: 'wire',
    tab: '4. Wire it',
    title: 'One composition root. Types inferred.',
    text: (
      <>
        A typed builder wires commands, queries, the projection and interceptors. The projection
        brings its own namespaces and subscription queries. Switching stores changes its inputs
        only; the same handlers run inline for tests or on tracking processors in production.
      </>
    ),
    link: ['/guide/async', 'Part 5: going asynchronous'],
    code: `let app = Cqrs::new(Events::new(store.clone()))
    .commands(|events| PostCommandGateway::new(posts_over(&store), events))
    .intercept(Audit(audit))
    .queries(PostQueryGateway::new(Arc::clone(&summaries)))
    .projection(Intercepted::new(projection, Measured(metrics))) // every namespace
    .build();

app.commands().send(PublishPost { id }).await?;
app.settled().await;                                  // read models caught up
let post = app.queries().query(FindPost { id }).await?;
let live = app.queries().subscribe(FindPost { id }); // then every change`,
  },
];

const FEATURES = [
  ['Messages & gateways', '/docs/patterns#messages-and-gateways', <>Commands and queries are plain structs that own their output and error types. A gateway struct implements a handler per message it lists in <code>handles(..)</code>.</>],
  ['Plain aggregates', '/docs/patterns#domain', <>Decisions take <code>&amp;self</code> and an appender; state changes only in <code>apply</code>. The domain barely knows the library.</>],
  ['Consistency', '/docs/storage', <>Optimistic concurrency per stream, plus dynamic consistency boundaries: tags, criteria and conditional appends.</>],
  ['Event processing', '/docs/event-processing', <>One projection per aggregate, one processor per namespace: read models replay, side effects do not. Inline or on tracking processors with segments, tokens and batches.</>],
  ['Unit of work', '/docs/unit-of-work', <>The library commits: a command's state and events in one transaction, a projection's read model with its position in another. Nothing is lost or applied twice.</>],
  ['Live queries', '/guide/live-queries', <>A projection lists its subscription queries and emits their new results; subscribers see their own writes, on every instance.</>],
  ['Interceptors', '/guide/interceptors', <>Around gateways and around event handlers, inside their transaction. A chain compiles to nested calls.</>],
  ['Storage', '/docs/storage#postgresql', <>In memory, and PostgreSQL over <code>sqlx</code> with compile-checked SQL, parallel appends and a gap-aware cursor.</>],
  ['Testing', '/docs/patterns#testing', <>A given/when/then fixture for aggregates and a tiny <code>block_on</code>: everything runs without an async runtime.</>],
];

const DOCS = [
  ['Start', 'Architecture', '/docs/architecture', 'The moving parts, and how a command becomes a read-model row.'],
  ['Tutorial', 'Guide', '/guide', 'A blog in nine parts, from the domain to a GraphQL API on PostgreSQL.'],
  ['Concepts', 'Patterns', '/docs/patterns', 'Every small design pattern the library is made of: problem, shape, rules.'],
  ['Reference', 'Unit of work', '/docs/unit-of-work', "Transactions, what commits when, and how it compares to Axon's."],
  ['Reference', 'Event processing', '/docs/event-processing', 'Projections and namespaces, tracking processors, segments, replay, dead letters.'],
  ['Reference', 'Storage', '/docs/storage', 'The store contract, in-memory and PostgreSQL, cursors, locks, snapshots, upcasting.'],
  ['Reference', 'Performance', '/docs/performance', 'Benchmarks against cqrs-es and the cost of each abstraction.'],
];

function Hero() {
  const logo = useBaseUrl('/img/logo-animated.svg');
  return (
    <header className={styles.hero}>
      <div className={clsx(styles.wrap, styles.heroInner)}>
        <div>
          <img className={styles.logo} src={logo} alt="" width="168" height="140" />
          <p className={styles.pill}>
            <span className={styles.dot} /> Pre-release · the API is still being shaped
          </p>
          <h1 className={styles.title}>
            CQRS and event sourcing for Rust, <em>resolved at compile time.</em>
          </h1>
          <p className={styles.lead}>
            A library, not a framework. Commands and queries are routed to their handlers by the
            compiler, projections commit with their position in one transaction, and everything is
            async from the ground up. Bring your own database, ORM and runtime.
          </p>
          <div className={styles.cta}>
            <Link className="button button--primary button--lg" to="/guide">
              Build a blog in 9 steps
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/introduction">
              Read the docs
            </Link>
          </div>
          <div className={styles.install}>
            <CodeBlock language="toml">{`cqrs-async = "0.1.0-alpha.1"           # use cqrs::...
cqrs-async-postgres = "0.1.0-alpha.1"  # use cqrs_postgres::...`}</CodeBlock>
          </div>
          <p className={styles.fine}>
            Not on crates.io yet: until then, depend on the crates by <code>path</code>.
          </p>
        </div>
        <div className={styles.heroCode}>
          <CodeBlock language="rust" title="retitle_post.rs">
            {HERO_CODE}
          </CodeBlock>
          <p className={styles.caption}>One impl per command. Find, decide, record: the library commits.</p>
        </div>
      </div>
    </header>
  );
}

function Principles() {
  return (
    <section className={styles.strip}>
      <div className={clsx(styles.wrap, styles.trio)}>
        <div>
          <h3>Checked by the compiler</h3>
          <p>
            No handler registry behind <code>dyn</code>, no <code>Any</code>, no <code>TypeId</code>. A
            missing or duplicated handler is a compile error, and the running code is plain function
            calls.
          </p>
        </div>
        <div>
          <h3>Your infrastructure</h3>
          <p>
            The core depends on no database, ORM or broker. Connections and transactions are yours;
            the library borrows them. Adapters adapt to your system, never the reverse.
          </p>
        </div>
        <div>
          <h3>Async, all the way</h3>
          <p>
            Every contract is a <code>Future</code> or a <code>Stream</code>. Committed events are a
            stream you can hand to axum, async-graphql or a Tokio task.
          </p>
        </div>
      </div>
    </section>
  );
}

function Features() {
  return (
    <section className={styles.section}>
      <div className={styles.wrap}>
        <h2>What you get</h2>
        <p className={styles.sub}>Small pieces that fit together, each one usable on its own.</p>
        <div className={styles.grid}>
          {FEATURES.map(([title, to, text]) => (
            <Link key={title} className={styles.card} to={to}>
              <h3>{title}</h3>
              <p>{text}</p>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

function CodeTour() {
  const [active, setActive] = useState(TOUR[0].id);
  const step = TOUR.find((t) => t.id === active);
  const onKey = (e, i) => {
    const next = e.key === 'ArrowRight' ? i + 1 : e.key === 'ArrowLeft' ? i - 1 : null;
    if (next === null) return;
    const target = TOUR[(next + TOUR.length) % TOUR.length];
    setActive(target.id);
    document.getElementById(`tab-${target.id}`)?.focus();
  };
  return (
    <section className={clsx(styles.section, styles.alt)}>
      <div className={styles.wrap}>
        <h2>From the domain to a running app</h2>
        <p className={styles.sub}>
          The blog from the <Link to="/guide">guide</Link>, in four pieces.
        </p>
        <div className={styles.tabs} role="tablist" aria-label="Code tour">
          {TOUR.map((t, i) => (
            <button
              key={t.id}
              id={`tab-${t.id}`}
              role="tab"
              aria-selected={t.id === active}
              aria-controls="tour-panel"
              tabIndex={t.id === active ? 0 : -1}
              className={clsx(styles.tab, t.id === active && styles.tabActive)}
              onClick={() => setActive(t.id)}
              onKeyDown={(e) => onKey(e, i)}>
              {t.tab}
            </button>
          ))}
        </div>
        <div className={styles.panel} id="tour-panel" role="tabpanel" aria-labelledby={`tab-${step.id}`}>
          <div>
            <h3>{step.title}</h3>
            <p>{step.text}</p>
            <p>
              <Link to={step.link[0]}>{step.link[1]} →</Link>
            </p>
          </div>
          <CodeBlock language="rust">{step.code}</CodeBlock>
        </div>
      </div>
    </section>
  );
}

function Flow() {
  return (
    <section className={styles.section}>
      <div className={styles.wrap}>
        <h2>One path from command to read model</h2>
        <p className={styles.sub}>
          The event store is the only source of truth. Everything on the read side can be rebuilt
          from it.
        </p>
        <div
          className={styles.flow}
          role="img"
          aria-label="A command goes through a gateway to a handler, which appends events to the event store. Tracking processors read the store and run projections in a unit of work with their token. Queries and subscription queries read the read model.">
          <div className={styles.lane}>
            <span className={styles.laneLabel}>Write</span>
            <div className={styles.node}>Command<small>gateway</small></div>
            <i className={styles.arrow} />
            <div className={styles.node}>Handler<small>find · decide · record</small></div>
            <i className={styles.arrow} />
            <div className={clsx(styles.node, styles.strong)}>Event store<small>append + condition</small></div>
          </div>
          <div className={styles.bridge}>
            <span>a processor per projection namespace · segments · tokens · batches · retries</span>
          </div>
          <div className={styles.lane}>
            <span className={styles.laneLabel}>Read</span>
            <div className={styles.node}>Projection<small>unit of work: handle + token</small></div>
            <i className={styles.arrow} />
            <div className={styles.node}>Read model<small>your repository</small></div>
            <i className={styles.arrow} />
            <div className={styles.node}>Query<small>or live subscription</small></div>
          </div>
        </div>
        <p className={styles.more}>
          <Link to="/docs/architecture">Read the architecture →</Link>
        </p>
      </div>
    </section>
  );
}

function Performance() {
  return (
    <section className={clsx(styles.section, styles.alt)}>
      <div className={styles.wrap}>
        <h2>Fast where it counts</h2>
        <p className={styles.sub}>
          Measured against <code>cqrs-es</code> 0.5 on the same bank scenario and the same
          PostgreSQL.
        </p>
        <div className={styles.stats}>
          <div className={styles.stat}><b>1.1×</b><span>open-account throughput</span></div>
          <div className={styles.stat}><b>1.4×</b><span>throughput under mixed load</span></div>
          <div className={styles.stat}><b>25×</b><span>read model rebuild, events/s</span></div>
          <div className={styles.stat}>
            <b>0</b>
            <span>
              events lost in the read model
              <br />
              <small>vs about 200 for cqrs-es under the same load</small>
            </span>
          </div>
        </div>
        <p className={clsx(styles.fine, styles.center)}>
          Our own benchmark, Apple M4 Pro, PostgreSQL 17 in Docker, 32 clients, median of 3 runs;
          treat differences under ~20% as noise. Routing a message through a gateway costs a few
          nanoseconds in memory. <Link to="/docs/performance">Method and every number →</Link>
        </p>
      </div>
    </section>
  );
}

function Concepts() {
  const rows = [
    ['Dispatch', 'CommandGateway', '#[cqrs::command_gateway], CommandGateway<C> bounds'],
    ['Command handler', '@CommandHandler', 'CommandHandler<C> impl on the gateway'],
    ['Recording events', 'EventAppender', 'CommandContext<E>: append, record'],
    ['Consistency boundary', 'EventCriteria', 'same names, CommandContext::source'],
    ['Event handling component', '@Namespace', 'a namespace of #[cqrs::projection]'],
    ['Event processor', 'PooledStreamingEventProcessor', 'TrackingProcessor'],
    ['Unit of work', 'ProcessingContext', "ProcessingContext<'_, Tx>"],
    ['Subscription queries', 'QueryUpdateEmitter', 'QueryUpdateEmitter<Self>, updates(..)'],
  ];
  const cell = (v) => (v.startsWith('same') || v.startsWith('a ') ? v : <code>{v}</code>);
  return (
    <section className={styles.section}>
      <div className={styles.wrap}>
        <h2>Coming from Axon?</h2>
        <p className={styles.sub}>
          The model is Axon Framework 5's, with the handler discovery done by the type system
          instead of reflection.
        </p>
        <div className={styles.tableWrap}>
          <table>
            <thead>
              <tr><th>Concept</th><th>Axon 5</th><th>cqrs-async</th></tr>
            </thead>
            <tbody>
              {rows.map(([concept, axon, ours]) => (
                <tr key={concept}>
                  <td><strong>{concept}</strong></td>
                  <td>{cell(axon)}</td>
                  <td>{cell(ours)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className={styles.more}>
          <Link to="/docs/architecture#concept-map">The full concept map →</Link>
        </p>
      </div>
    </section>
  );
}

function Docs() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <section className={clsx(styles.section, styles.alt)}>
      <div className={styles.wrap}>
        <h2>Documentation</h2>
        <p className={styles.sub}>{siteConfig.tagline}</p>
        <div className={clsx(styles.grid, styles.four)}>
          {DOCS.map(([kicker, title, to, text]) => (
            <Link key={title} className={styles.card} to={to}>
              <span className={styles.kicker}>{kicker}</span>
              <h3>{title}</h3>
              <p>{text}</p>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title="CQRS and event sourcing for Rust" description={siteConfig.tagline}>
      <Hero />
      <main>
        <Principles />
        <Features />
        <CodeTour />
        <Flow />
        <Performance />
        <Concepts />
        <Docs />
      </main>
    </Layout>
  );
}

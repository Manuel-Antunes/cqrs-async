//! Fan-in: a projection over two logs, each aggregate root appending to its
//! own, read as one in their global order through an enum with a variant
//! per log's event type.

#![cfg(feature = "tokio")]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    aggregate::{Loaded, New},
    command::{CommandContext, CommandGateway, CommandHandler},
    event::{DomainEvent, EventEnvelope, Metadata, StreamId},
    processing::{EventHandler, Events, HandlerError, ProcessingContext},
    store::{AppendEvents, ExpectedVersion, FanIn, InMemoryEventStore, ReadLog, StoreError, SubscriptionOptions},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum PostEvent {
    #[event(name = "post.published")]
    Published { author: u32, title: String },
}

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum UserEvent {
    #[event(name = "user.registered")]
    Registered { name: String },
    #[event(name = "user.renamed", version = 2)]
    Renamed {
        #[tag]
        name: String,
    },
}

/// What the feed reads: the posts' log and the users', in one order.
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
#[event(fan_in)]
enum FeedEvent {
    Post(PostEvent),
    User(UserEvent),
}

#[cqrs::aggregate(name = "post", event = PostEvent)]
struct Post {
    id: u32,
}

impl Post {
    fn apply(&mut self, _: &PostEvent) {}
}

#[cqrs::aggregate(name = "user", event = UserEvent)]
struct User {
    id: u32,
}

impl User {
    fn apply(&mut self, _: &UserEvent) {}
}

#[derive(Debug, PartialEq)]
struct Refused(String);

impl From<StoreError> for Refused {
    fn from(error: StoreError) -> Self {
        Self(error.to_string())
    }
}

#[cqrs::command(error = Refused)]
struct Publish {
    id: u32,
    author: u32,
    title: &'static str,
}

#[cqrs::command(error = Refused)]
struct Register {
    id: u32,
    name: &'static str,
}

#[cqrs::command(error = Refused)]
struct Rename {
    id: u32,
    name: &'static str,
}

type Posts = Events<PostEvent, InMemoryEventStore<PostEvent>>;
type Users = Events<UserEvent, InMemoryEventStore<UserEvent>>;

#[cqrs::command_gateway(handles(Publish))]
struct PostCommands {
    #[gateway(unit)]
    events: Posts,
}

#[cqrs::command_handler]
impl CommandHandler<Publish> for PostCommands {
    async fn handle(&self, command: &Publish, ctx: &mut CommandContext<PostEvent>) -> Result<(), Refused> {
        ctx.append(PostEvent::Published {
            author: command.author,
            title: command.title.to_owned(),
        });
        ctx.commit(New::<Post>(command.id));
        Ok(())
    }
}

#[cqrs::command_gateway(handles(Register, Rename))]
struct UserCommands {
    #[gateway(unit)]
    events: Users,
}

#[cqrs::command_handler]
impl CommandHandler<Register> for UserCommands {
    async fn handle(&self, command: &Register, ctx: &mut CommandContext<UserEvent>) -> Result<(), Refused> {
        ctx.append(UserEvent::Registered {
            name: command.name.to_owned(),
        });
        ctx.commit(New::<User>(command.id));
        Ok(())
    }
}

#[cqrs::command_handler]
impl CommandHandler<Rename> for UserCommands {
    async fn handle(&self, command: &Rename, ctx: &mut CommandContext<UserEvent>) -> Result<(), Refused> {
        ctx.append(UserEvent::Renamed {
            name: command.name.to_owned(),
        });
        ctx.commit(Loaded::at(User { id: command.id }, 1));
        Ok(())
    }
}

/// The feed: each post with its author's current name, which only the
/// users' log has.
#[derive(Default)]
struct Feed {
    names: HashMap<u32, String>,
    /// (title, author id)
    posts: Vec<(String, u32)>,
}

#[cqrs::projection(name = "feed", events = FeedEvent, namespaces("feed"))]
#[derive(Default)]
struct FeedProjection {
    feed: Mutex<Feed>,
}

#[cqrs::event_handler(namespace = "feed")]
impl EventHandler for FeedProjection {
    async fn handle(
        &self,
        event: &EventEnvelope<FeedEvent>,
        _: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        let mut feed = self.feed.lock().unwrap();
        match &event.payload {
            FeedEvent::User(UserEvent::Registered { name } | UserEvent::Renamed { name }) => {
                let user = event
                    .aggregate_id::<User>()
                    .ok_or_else(|| HandlerError::msg("not a user"))?;
                feed.names.insert(user, name.clone());
            }
            FeedEvent::Post(PostEvent::Published { author, title }) => {
                // The author was registered before: the global order says so.
                if !feed.names.contains_key(author) {
                    return Err(HandlerError::msg("a post before its author"));
                }
                feed.posts.push((title.clone(), *author));
            }
        }
        Ok(())
    }
}

impl FeedProjection {
    /// The feed as shown: titles with their authors' current names.
    fn shown(&self) -> Vec<(String, String)> {
        let feed = self.feed.lock().unwrap();
        feed.posts
            .iter()
            .map(|(title, author)| (title.clone(), feed.names[author].clone()))
            .collect()
    }
}

/// The positions of `events`.
fn positions<E>(events: Vec<EventEnvelope<E>>) -> Vec<u64> {
    events.iter().map(|event| event.position).collect()
}

#[test]
fn a_fan_in_event_is_the_event_it_wraps() {
    let renamed = FeedEvent::from(UserEvent::Renamed { name: "Ana".into() });
    assert_eq!(renamed.event_type(), "user.renamed");
    assert_eq!(renamed.event_version(), 2);
    assert_eq!(
        cqrs::event::TagResolver::tags(&renamed),
        [cqrs::event::Tag::new("name", "Ana")]
    );
    let published: FeedEvent = PostEvent::Published {
        author: 1,
        title: "Hello".into(),
    }
    .into();
    assert_eq!(published.event_type(), "post.published");
    assert_eq!(published.event_version(), 1);
}

#[tokio::test]
async fn logs_made_beside_each_other_are_read_as_one_in_order() {
    let posts = InMemoryEventStore::<PostEvent>::new();
    let users = posts.beside::<UserEvent>();
    let append_user = async |id: u32, name: &str| {
        users
            .append(
                &mut (),
                &StreamId::new("user", id.to_string()),
                ExpectedVersion::Any,
                vec![UserEvent::Registered { name: name.into() }],
                &Metadata::new(),
            )
            .await
            .unwrap();
    };
    append_user(1, "Ana").await;
    posts
        .append(
            &mut (),
            &StreamId::new("post", "1"),
            ExpectedVersion::NoStream,
            vec![
                PostEvent::Published {
                    author: 1,
                    title: "a".into(),
                },
                PostEvent::Published {
                    author: 1,
                    title: "b".into(),
                },
            ],
            &Metadata::new(),
        )
        .await
        .unwrap();
    append_user(2, "Bia").await;

    // Each log keeps its own events, numbered in the shared order.
    assert_eq!(positions(users.read_all(0, 10).await.unwrap()), [1, 4]);
    assert_eq!(positions(posts.read_all(0, 10).await.unwrap()), [2, 3]);
    assert_eq!(posts.last_position().await.unwrap(), 3);

    let feed: FanIn<FeedEvent, _> = FanIn::new((posts.clone(), users.clone()));
    let all = feed.read_all(0, 10).await.unwrap();
    assert_eq!(positions(all.clone()), [1, 2, 3, 4]);
    assert!(matches!(all[1].payload, FeedEvent::Post(_)));
    assert_eq!(all[3].stream, StreamId::new("user", "2"));
    // Paged, the merge loses nothing.
    assert_eq!(positions(feed.read_all(1, 2).await.unwrap()), [2, 3]);
    assert_eq!(positions(feed.read_all(3, 2).await.unwrap()), [4]);
    assert_eq!(feed.last_position().await.unwrap(), 4);
}

#[test]
#[should_panic(expected = "share their numbering")]
fn logs_numbered_apart_cannot_be_read_as_one() {
    let posts = InMemoryEventStore::<PostEvent>::new();
    let users = InMemoryEventStore::<UserEvent>::new();
    let _: FanIn<FeedEvent, _> = FanIn::new((posts, users));
}

#[tokio::test]
async fn a_projection_over_two_logs_sees_their_events_in_commit_order() {
    let posts = Posts::new(InMemoryEventStore::new());
    let users = Users::new(posts.store().beside());
    // A poll far away: only the logs' commits wake the feed in time.
    let feed: Events<FeedEvent, _> = Events::fan_in((&posts, &users))
        .with_options(SubscriptionOptions::default().poll_interval(Duration::from_secs(60)));
    let projection = Arc::new(FeedProjection::default());
    let running = feed.projection(Arc::clone(&projection)).spawn();

    let post_commands = PostCommands { events: posts.clone() };
    let user_commands = UserCommands { events: users.clone() };
    user_commands.send(Register { id: 7, name: "Ana" }).await.unwrap();
    post_commands
        .send(Publish {
            id: 1,
            author: 7,
            title: "Hello",
        })
        .await
        .unwrap();
    user_commands
        .send(Rename {
            id: 7,
            name: "Ana Maria",
        })
        .await
        .unwrap();

    let last = feed.store().last_position().await.unwrap();
    assert_eq!(last, 3);
    tokio::time::timeout(Duration::from_secs(5), running.caught_up(last))
        .await
        .expect("the logs' commits woke the feed");
    assert_eq!(projection.shown(), [("Hello".to_owned(), "Ana Maria".to_owned())]);
    running.stop().await;
}

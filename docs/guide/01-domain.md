# 1. The domain

The domain is the business rules and nothing else: no store, no bus, no
runtime. A post is drafted with a title, may be retitled while it is a
draft, and is then published, after which it is frozen.

## Setup

```toml
[package]
name = "blog"
edition = "2024"

[dependencies]
cqrs = { path = "../../crates/cqrs" }    # derive + tokio by default
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

## The identity

```rust
// src/domain/post/id.rs
use std::fmt;
use serde::{Deserialize, Serialize};

/// Its `Display` form (`42`) is the suffix of the post's stream (`post-42`),
/// so it must never change for a stored post.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PostId(pub u64);

impl fmt::Display for PostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Back from the `Display` form (a stream's suffix): projections use it.
impl std::str::FromStr for PostId {
    type Err = std::num::ParseIntError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}
```

## The events

Events are facts, in the past tense, one enum per aggregate (each aggregate's
events get their own log in PostgreSQL: part 8):

```rust
// src/domain/post/event.rs
use super::PostId;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
pub enum PostEvent {
    /// Always the first event of a post's stream.
    #[event(name = "post.created")]
    Created { id: PostId, title: String },
    #[event(name = "post.retitled")]
    Retitled { title: String },
    /// No event follows it.
    #[event(name = "post.published")]
    Published,
}
```

- `DomainEvent` gives each variant its stored type name. Name them
  explicitly: the default is the Rust name, and renaming a variant would
  then break the stored events. Each variant also has a schema version
  (`#[event(version = 2)]`, default 1), for [upcasting](../storage.md#upcasting).
- `TagResolver` says which tags an event carries. A post's events need
  none (they are found by stream), but every event declares its tags; the
  derive with no `#[tag]` fields means "none".
- serde is for stores that serialize (PostgreSQL, part 8).

## The errors

```rust
// src/domain/post/error.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostError {
    EmptyTitle,
    SameTitle,
    AlreadyPublished,
    PublishedPostsAreFrozen,
}
// + Display and std::error::Error
```

## The aggregate

```rust
// src/domain/post/mod.rs
use cqrs::EventAppender;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::AggregateRoot)]
#[aggregate(name = "post", event = PostEvent)]
pub struct Post {
    id: PostId,
    title: String,
    published: bool,
}

impl Post {
    /// How an event changes the post, when it is recorded and when it is
    /// replayed: no validation, no failure.
    fn apply(&mut self, event: &PostEvent) {
        match event {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => self.title.clone_from(title),
            PostEvent::Published => self.published = true,
        }
    }

    /// Drafts the post `id`. There is no post yet, so there is no `self`
    /// (Axon 5's static creational command handler).
    pub fn create(id: PostId, title: impl Into<String>, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
        let title = valid_title(title.into())?;
        events.append(PostEvent::Created { id, title });
        Ok(())
    }

    pub fn retitle(&self, title: impl Into<String>, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
        if self.published {
            return Err(PostError::PublishedPostsAreFrozen);
        }
        let title = valid_title(title.into())?;
        if title == self.title {
            return Err(PostError::SameTitle);
        }
        events.append(PostEvent::Retitled { title });
        Ok(())
    }

    pub fn publish(&self, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
        if self.published {
            return Err(PostError::AlreadyPublished);
        }
        events.append(PostEvent::Published);
        Ok(())
    }

    pub fn title(&self) -> &str { &self.title }
    pub fn is_published(&self) -> bool { self.published }
}

fn valid_title(title: String) -> Result<String, PostError> {
    let title = title.trim();
    if title.is_empty() { Err(PostError::EmptyTitle) } else { Ok(title.to_owned()) }
}
```

What the derive writes, and you could write instead:

```rust
impl cqrs::Aggregate for Post {
    const TYPE: &'static str = "post";             // stream prefix: post-42
    type Id = PostId;                              // the `id` field
    type Event = PostEvent;
    fn initial(id: &PostId) -> Self {              // where replay starts: other fields at Default
        Self { id: *id, title: String::new(), published: false }
    }
    fn id(&self) -> &PostId { &self.id }
    fn evolve(&mut self, event: &PostEvent) { self.apply(event) }
}
```

The rules this shape enforces:

- **State changes only through events.** Decisions take `&self`;
  assigning `self.published = true` in `publish` does not compile.
- **Creating takes no post.** `create` is an associated function: there is
  no post to decide on yet, and none can be created twice by calling it on
  one that exists. `initial` is only where replay starts; the commit of a
  creation targets `cqrs::New`, which expects the stream not to exist.
- **No buffer in the aggregate.** Decisions append to the appender they
  are given (Axon 5's `EventAppender`); the struct stays plain data, so
  derives and serde work as usual.
- **A decision sees the state from before it ran.** The appended events
  are applied when they are committed.
- **The domain barely knows the framework.** `EventAppender<E>` is
  implemented for every `Extend<E>`, so a decision could take
  `&mut impl Extend<PostEvent>` and the domain would not import `cqrs` at
  all (beyond the derives).

## Testing the rules

Given past events, when a decision runs, expect events or an error. No
store, no async:

```rust
// tests/domain.rs
use blog::domain::{Post, PostError, PostEvent, PostId};
use cqrs::testing::AggregateFixture;

const ID: PostId = PostId(7);

fn created() -> PostEvent {
    PostEvent::Created { id: ID, title: "Hello".into() }
}

#[test]
fn creating_a_post_appends_created() {
    AggregateFixture::<Post>::creating(ID, |events| Post::create(ID, "  Hello ", events))
        .expect_events([created()])
        .expect_state(|post| assert_eq!(post.title(), "Hello"));
}

#[test]
fn a_post_is_published_once() {
    AggregateFixture::<Post>::new(ID)
        .given([created(), PostEvent::Published])
        .when(|post, events| post.publish(events))
        .expect_error(PostError::AlreadyPublished)
        .expect_no_events();
}

#[test]
fn a_published_post_is_frozen() {
    AggregateFixture::<Post>::new(ID)
        .given([created(), PostEvent::Published])
        .when(|post, events| post.retitle("Other", events))
        .expect_error(PostError::PublishedPostsAreFrozen);
}
```

Next: [2. Commands](02-commands.md).

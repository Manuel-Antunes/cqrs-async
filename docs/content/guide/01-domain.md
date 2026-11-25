---
title: "1. The domain"
sidebar_position: 11
---

The domain is the business rules and nothing else: no store, no bus, no
runtime. A post is drafted with a title, may be retitled while it is a
draft, and is then published, after which it is frozen.

## Setup

```toml
[package]
name = "blog"
edition = "2024"

[dependencies]
cqrs-async = "0.1.0-alpha.1"   # `cqrs::` in code; "derive" and "tokio" are on by default
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## The identity

```rust
// src/domain/post/id.rs
use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a post. Its `Display` form (`42`) is the suffix of the post's
/// event stream (`post-42`), so it must never change for a stored post.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PostId(pub u64);

impl fmt::Display for PostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Back from the `Display` form (a stream's suffix).
impl std::str::FromStr for PostId {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}
```

The `FromStr` is the way back: a projection reads a post's id from its
event with `event.aggregate_id::<Post>()`, which parses the stream's suffix.

## The events

Events are facts, in the past tense, one enum per aggregate (in
PostgreSQL, each aggregate's events get a log of their own: part 8):

```rust
// src/domain/post/event.rs
use super::PostId;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, cqrs::DomainEvent)]
pub enum PostEvent {
    /// A draft was created. Always the first event of a post's stream.
    #[event(name = "post.created")]
    Created { id: PostId, title: String },
    /// The draft's title changed.
    #[event(name = "post.retitled")]
    Retitled { title: String },
    /// The post became public; no event follows it.
    #[event(name = "post.published")]
    Published,
}
```

- `DomainEvent` gives each variant its stored type name. Name them
  explicitly: the default is the Rust name, so renaming a variant would
  break the events already stored. Each variant also has a schema version
  (`#[event(version = 2)]`, default 1), for
  [upcasting](../docs/storage.md#upcasting).
- serde is for stores that serialize (PostgreSQL, part 8). The in-memory
  store does not need it.

### Tags

Every event has tags (`TagResolver`, which every `DomainEvent` has), and
the derive writes them from the fields marked `#[tag]`: each becomes a tag
keyed by the field's name, valued by its `Display`. A post's events need
none: a post is found by its stream, `post-42`. So the blog marks no field,
and its events have no tags.

Tags matter when a decision spans more than one stream. This is the
library's own test of dynamic consistency boundaries: a subscription counts
for a course and for a student, so it is tagged with both:

```rust
// crates/cqrs/tests/dcb.rs
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum SchoolEvent {
    #[event(name = "course.opened")]
    CourseOpened {
        #[tag]
        course: u32,
        capacity: usize,
    },
    #[event(name = "student.subscribed")]
    Subscribed {
        #[tag]
        course: u32,
        #[tag]
        student: u32,
    },
}

/// Every event tagged with its id: its opening and its subscriptions.
#[cqrs::aggregate(event = SchoolEvent, tag = "course")]
struct Course { id: u32, capacity: usize, students: Vec<u32> }
```

`tag = "course"` builds a `Course` from every event tagged `course=<id>`,
whichever stream holds it, and its writes are refused if any such event
was appended since it was loaded. See [Patterns](../docs/patterns.md) for
the details; the blog does not need them.

## The errors

```rust
// src/domain/post/error.rs
/// A business rule the post refused to break. Nothing is committed when an
/// operation returns one of these.
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
use cqrs::aggregate::EventAppender;
use serde::{Deserialize, Serialize};

#[cqrs::aggregate(name = "post", event = PostEvent)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Post {
    id: PostId,
    title: String,
    published: bool,
}

impl Post {
    /// How an event changes the post, both when it is committed and when it
    /// is replayed: no validation, no failure.
    fn apply(&mut self, event: &PostEvent) {
        match event {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => {
                self.title.clone_from(title);
            }
            PostEvent::Published => self.published = true,
        }
    }

    /// Drafts the post `id`, unpublished. There is no post yet, so this
    /// takes none: commit it to `cqrs::aggregate::New`, which expects the
    /// post's stream not to exist.
    pub fn create(
        id: PostId,
        title: impl Into<String>,
        events: &mut impl EventAppender<PostEvent>,
    ) -> Result<(), PostError> {
        let title = valid_title(title.into())?;
        events.append(PostEvent::Created { id, title });
        Ok(())
    }

    pub fn retitle(
        &self,
        title: impl Into<String>,
        events: &mut impl EventAppender<PostEvent>,
    ) -> Result<(), PostError> {
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

    /// The post as a repository that keeps state stored it (rather than
    /// rebuilding it from its events).
    pub fn restore(id: PostId, title: String, published: bool) -> Self {
        Self { id, title, published }
    }

    pub fn title(&self) -> &str { &self.title }
    pub fn is_published(&self) -> bool { self.published }
}

fn valid_title(title: String) -> Result<String, PostError> {
    let title = title.trim();
    if title.is_empty() { Err(PostError::EmptyTitle) } else { Ok(title.to_owned()) }
}
```

`#[cqrs::aggregate]` writes the `Aggregate` impl. You could write it
instead:

```rust
impl cqrs::aggregate::Aggregate for Post {
    const TYPE: &'static str = "post";             // stream prefix: post-42
    type Id = PostId;                              // the `id` field (or one marked #[aggregate(id)])
    type Event = PostEvent;
    fn initial(id: &PostId) -> Self {              // where replay starts: other fields at Default
        Self { id: id.clone(), title: Default::default(), published: Default::default() }
    }
    fn id(&self) -> &PostId { &self.id }
    fn evolve(&mut self, event: &PostEvent) { Self::apply(self, event) }
}
```

The rules this shape enforces:

- **State changes only through events.** Decisions take `&self`;
  assigning `self.published = true` in `publish` does not compile.
- **Creating takes no post.** `create` is an associated function (Axon 5's
  static creational command handler): there is no post to decide on yet,
  and none can be created twice by calling it on one that exists. Its
  events are committed to `New::<Post>(id)`, which expects the stream not
  to exist (part 2). `initial` is only where replay starts.
- **No buffer in the aggregate.** Decisions append to the appender they
  are given (Axon 5's `EventAppender`); the struct stays plain data, so
  derives and serde work as usual.
- **A decision sees the state from before it ran.** The appended events
  are applied when they are committed.
- **The domain barely knows the framework.** `EventAppender<E>` is
  implemented for every `Extend<E>`, so a decision could take
  `&mut impl Extend<PostEvent>` and the domain would import nothing from
  `cqrs` but its macros.

`restore` is for a repository that keeps a post's state in a table instead
of rebuilding it from events (part 8). It is not a way around the rules:
every change still goes through a decision and its events.

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

`creating` runs a creation on no post; `new(..).given(..)` replays past
events first.

Next: [2. Commands](02-commands.md).

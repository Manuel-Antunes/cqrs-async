//! End to end: command gateway → handler → repository port → event store →
//! processors (projection, policy) → read model → query gateway.
//! Runs on `cqrs::testing::block_on`, no async runtime involved.

use blog::{
    PostStore,
    application::{BlogError, commands::*, queries::*},
    domain::{PostError, PostId},
    in_memory, in_memory_with,
    infrastructure::EventSourcedPostRepository,
};
use cqrs::{CommandGateway, EventPublisher, InlinePublisher, Pending, QueryGateway, StoreError, testing::block_on};
use std::sync::Arc;

fn assert_send<T: Send>(value: T) -> T {
    value
}

/// Creates a post and publishes it right away, returning its id.
///
/// Depends only on the ports it uses, like a controller or a saga would: any
/// gateway that can send both commands fits, including a test double.
///
/// # Errors
///
/// The first failing command's error. If publishing fails, the post stays
/// created as a draft: the two commands are not atomic.
pub async fn create_and_publish<G>(gateway: &G, title: &str) -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{
    let id = gateway
        .send(CreatePost {
            title: title.to_owned(),
        })
        .await?;
    gateway.send(PublishPost { id }).await?;
    Ok(id)
}

#[test]
fn a_command_commits_events_and_the_projection_answers_queries() {
    block_on(async {
        let (blog, _, notifier) = in_memory();

        let id = blog
            .commands()
            .send(CreatePost {
                title: "Static dispatch".into(),
            })
            .await
            .unwrap();
        blog.commands().send(PublishPost { id }).await.unwrap();

        let found = blog.queries().query(FindPost { id }).await.unwrap();
        assert_eq!(
            found,
            Some(PostSummary {
                id,
                title: "Static dispatch".into(),
                published: true,
            })
        );
        assert_eq!(blog.queries().query(ListPublished).await.unwrap().len(), 1);
        assert_eq!(notifier.sent(), ["post-1 published"]);
    });
}

#[test]
fn the_dispatch_futures_are_send() {
    let (blog, _, _) = in_memory();
    // Compiles only if the whole chain (gateway, handler, repository, store,
    // processors) produces `Send` futures: ready for `tokio::spawn`.
    let future = assert_send(blog.commands().send(CreatePost { title: "x".into() }));
    assert_eq!(block_on(future), Ok(PostId(1)));
}

#[test]
fn domain_errors_reach_the_caller_and_nothing_is_stored() {
    block_on(async {
        let store = PostStore::default();
        let (blog, summaries, _) = in_memory_with(store.clone());
        let error = blog.commands().send(CreatePost { title: " ".into() }).await;
        assert_eq!(error, Err(BlogError::Domain(PostError::EmptyTitle)));
        assert!(store.all().is_empty());
        assert!(summaries.published().await.unwrap().is_empty());
    });
}

#[test]
fn unknown_aggregates_are_reported() {
    block_on(async {
        let (blog, _, _) = in_memory();
        let error = blog.commands().send(PublishPost { id: PostId(99) }).await;
        assert_eq!(error, Err(BlogError::NotFound(PostId(99))));
    });
}

#[test]
fn concurrent_writers_cannot_overwrite_each_other() {
    block_on(async {
        let store = PostStore::default();
        let (blog, _, _) = in_memory_with(store.clone());
        let id = blog.commands().send(CreatePost { title: "v1".into() }).await.unwrap();
        // Two writers of their own, over the blog's store.
        let events = InlinePublisher::new(store.clone());
        let posts = EventSourcedPostRepository::new(store, InlinePublisher::new(PostStore::default()));

        // Two writers load the same version and decide...
        let first = posts.find(id).await.unwrap().unwrap();
        let second = posts.find(id).await.unwrap().unwrap();
        let (mut renamed, mut published) = (Vec::new(), Vec::new());
        first.retitle("v2", &mut renamed).unwrap();
        second.publish(&mut published).unwrap();

        // ...the first one wins, the second gets a conflict.
        events.commit(first, Pending::new(renamed)).await.unwrap();
        let error = events.commit(second, Pending::new(published)).await.err().unwrap();
        assert!(matches!(error, StoreError::Conflict { actual: Some(2), .. }));

        let stored = posts.find(id).await.unwrap().unwrap();
        assert_eq!(stored.title(), "v2");
        assert!(!stored.is_published());
        assert_eq!(stored.version(), Some(2));
    });
}

#[test]
fn a_failing_policy_behind_the_gateway_does_not_fail_the_command() {
    block_on(async {
        let (blog, summaries, notifier) = in_memory();
        let id = blog.commands().send(CreatePost { title: "t".into() }).await.unwrap();
        notifier.set_down(true);

        // The event is committed and the projection ran; the notification
        // policy failed, which the engine's `on_failure` policy handles
        // (it logs), not the command's sender.
        blog.commands().send(PublishPost { id }).await.unwrap();
        assert!(summaries.find(id).await.unwrap().unwrap().published);
        assert!(notifier.sent().is_empty());
    });
}

#[test]
fn replaying_the_log_into_the_projection_rebuilds_the_same_rows() {
    use blog::application::{projections::PostSummaryProjection, queries::PostUpdates};
    use cqrs::{EventHandler, ProcessingContext};
    use std::sync::Arc;
    block_on(async {
        let store = PostStore::default();
        let (blog, summaries, _) = in_memory_with(store.clone());
        let id = blog.commands().send(CreatePost { title: "a".into() }).await.unwrap();
        blog.commands()
            .send(RetitlePost { id, title: "b".into() })
            .await
            .unwrap();

        // A replay: cleared, then every event again, in order.
        let projection = PostSummaryProjection::new(Arc::clone(&summaries), Arc::new(PostUpdates::default()));
        projection
            .reset(&mut ProcessingContext::new(&mut (), false))
            .await
            .unwrap();
        for event in store.all() {
            projection
                .handle(&event, &mut ProcessingContext::new(&mut (), true))
                .await
                .unwrap();
        }
        assert_eq!(summaries.find(id).await.unwrap().unwrap().title, "b");
    });
}

#[test]
fn handlers_can_be_called_directly_without_any_bus() {
    use cqrs::{InMemoryEventStore, Metadata, dispatch_command};

    block_on(async {
        // A handler holds the repository it saves through: call it as is.
        let store = InMemoryEventStore::new();
        let posts = EventSourcedPostRepository::new(store.clone(), InlinePublisher::new(store));
        let handler = CreatePostHandler::new(Arc::new(posts));
        let id = dispatch_command(&handler, CreatePost { title: "direct".into() }, Metadata::new())
            .await
            .unwrap();
        assert_eq!(id, PostId(1));
    });
}

#[test]
fn components_depend_on_gateway_ports() {
    block_on(async {
        let (blog, summaries, _) = in_memory();
        let id = create_and_publish(blog.commands(), "via port").await.unwrap();
        assert!(summaries.find(id).await.unwrap().unwrap().published);
    });
}

#[test]
fn a_gateway_can_be_written_by_hand_without_the_derive() {
    use blog::{
        application::{projections::PostSummaryProjection, queries::PostUpdates},
        domain::PostId,
        infrastructure::InMemoryPostSummaryRepository,
    };
    use cqrs::{Dispatch, Handler, InMemoryEventStore, Metadata, dispatch_command};

    /// What `#[derive(Gateway)]` writes, one `Dispatch` impl per message.
    struct ByHand<R> {
        create: CreatePostHandler<R>,
        publish: PublishPostHandler<R>,
        find: FindPostHandler<InMemoryPostSummaryRepository>,
    }

    impl<R: PostRepository> Dispatch<CreatePost> for ByHand<R> {
        async fn dispatch_with(&self, message: CreatePost, metadata: Metadata) -> Result<PostId, BlogError> {
            dispatch_command(&self.create, message, metadata).await
        }
    }

    impl<R: PostRepository> Dispatch<PublishPost> for ByHand<R> {
        async fn dispatch_with(&self, message: PublishPost, metadata: Metadata) -> Result<(), BlogError> {
            dispatch_command(&self.publish, message, metadata).await
        }
    }

    impl<R: Send + Sync> Dispatch<FindPost> for ByHand<R> {
        async fn dispatch_with(
            &self,
            message: FindPost,
            metadata: Metadata,
        ) -> Result<Option<blog::application::queries::PostSummary>, BlogError> {
            self.find.handle_with(message, metadata).await
        }
    }

    block_on(async {
        let store = InMemoryEventStore::new();
        let summaries = Arc::new(InMemoryPostSummaryRepository::default());
        let publisher = InlinePublisher::new(store.clone()).with_processor((PostSummaryProjection::new(
            summaries.clone(),
            Arc::new(PostUpdates::default()),
        ),));
        let posts = Arc::new(EventSourcedPostRepository::new(store, publisher));
        let gateway = ByHand {
            create: CreatePostHandler::new(Arc::clone(&posts)),
            publish: PublishPostHandler::new(posts),
            find: FindPostHandler::new(summaries),
        };

        let id = gateway
            .send(CreatePost {
                title: "by hand".into(),
            })
            .await
            .unwrap();
        gateway.send(PublishPost { id }).await.unwrap();
        let found = gateway.query(FindPost { id }).await.unwrap().unwrap();
        assert!(found.published);
    });
}

#[test]
fn the_command_and_query_enums_route_each_variant_to_its_handler() {
    block_on(async {
        let (blog, _, _) = in_memory();

        // As a transport would: deserialized, then sent as one message type.
        let created: PostCommand = serde_json::from_str(r#"{"CreatePost": {"title": "Over the wire"}}"#).unwrap();
        let PostCommandOutput::CreatePost(id) = blog.commands().send(created).await.unwrap() else {
            panic!("a CreatePost answers with its id");
        };
        let publish = PostCommand::from(PublishPost { id });
        assert_eq!(
            blog.commands().send(publish).await,
            Ok(PostCommandOutput::PublishPost(()))
        );
        // Errors come back as the enum's error.
        let again = blog.commands().send(PostCommand::from(PublishPost { id })).await;
        assert_eq!(again, Err(BlogError::Domain(PostError::AlreadyPublished)));

        let found = blog.queries().query(PostQuery::from(FindPost { id })).await.unwrap();
        let PostQueryOutput::FindPost(Some(summary)) = found else {
            panic!("the post is projected in-process");
        };
        assert!(summary.published);
        assert_eq!(
            serde_json::to_string(&PostCommand::from(PublishPost { id })).unwrap(),
            r#"{"PublishPost":{"id":1}}"#
        );
    });
}

use super::PostId;

/// What happened to a post. Each variant is one stored fact; its `name` is
/// the stable event type written to the store, independent of the Rust name.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, cqrs::DomainEvent)]
pub enum PostEvent {
    /// A draft was created. Always the first event of a post's stream.
    #[event(name = "post.created")]
    Created {
        /// The new post's id, so readers of the event need not parse the
        /// stream id.
        id: PostId,
        /// The initial, already validated title.
        title: String,
    },
    /// The draft's title changed.
    #[event(name = "post.retitled")]
    Retitled {
        /// The new, already validated title.
        title: String,
    },
    /// The post became public; no event follows it.
    #[event(name = "post.published")]
    Published,
}

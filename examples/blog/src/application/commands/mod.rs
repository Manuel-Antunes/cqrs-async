//! Commands: messages that change state. One file per use case, each with
//! its message and its handler, an impl on [`PostCommandGateway`].

/// Drafting a new post.
mod create_post;
/// Where the handlers find posts (a port).
mod post_repository;
/// Making a draft public.
mod publish_post;
/// Renaming a draft.
mod retitle_post;

pub use create_post::CreatePost;
pub use post_repository::PostRepository;
pub use publish_post::PublishPost;
pub use retitle_post::RetitlePost;

/// Write side: handles every command of the posts, one `CommandHandler`
/// impl each (in the use case's file), sharing the repository. Each command
/// listed is routed to its impl at compile time (no registry, no `dyn`); one
/// listed without its impl does not compile. Also generates
/// [`PostCommands`], the trait of whatever routes them all.
#[cqrs::command_gateway(commands = PostCommands, handles(CreatePost, RetitlePost, PublishPost))]
pub struct PostCommandGateway<R, P> {
    /// Where posts are found and kept, and where new ones get their ids.
    posts: R,
    /// The unit of work every command runs in: it opens the transaction
    /// the handlers and the repository write in, appends what they
    /// recorded, and commits.
    #[gateway(unit)]
    events: P,
}

impl<R, P> PostCommandGateway<R, P> {
    /// The gateway over the posts' repository, its commands running in
    /// units of work of `events`.
    pub fn new(posts: R, events: P) -> Self {
        Self { posts, events }
    }
}

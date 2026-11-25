use cqrs::{
    aggregate::New,
    command::{CommandContext, CommandHandler},
};

use crate::{
    application::{
        BlogError,
        commands::{PostCommandGateway, PostRepository},
    },
    domain::{Post, PostEvent, PostId},
};

/// Creates a draft post and returns its id.
#[cqrs::command(output = PostId, error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CreatePost {
    /// The title; trimmed, and refused if blank.
    pub title: String,
}

/// Takes the next id from the repository, lets the domain create the post
/// and saves it (expecting it not to exist yet).
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<CreatePost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &CreatePost,
        ctx: &mut CommandContext<PostEvent, R::Tx>,
    ) -> Result<PostId, BlogError> {
        let id = self.posts.next_id(ctx.tx()).await?;
        Post::create(id, command.title.as_str(), ctx)?;
        let post = ctx.commit(New::<Post>(id));
        self.posts.save(&post, ctx.tx()).await?;
        Ok(id)
    }
}

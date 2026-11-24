use cqrs::{CommandContext, CommandHandler, NotFound};

use crate::{
    application::{
        BlogError,
        commands::{PostCommandGateway, PostRepository},
    },
    domain::{PostEvent, PostId},
};

/// Changes the title of a draft.
#[cqrs::command(error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RetitlePost {
    /// The post to rename.
    pub id: PostId,
    /// The new title; trimmed, and refused if blank or unchanged.
    pub title: String,
}

/// Loads the post, lets the domain rename it and saves the result; nothing
/// is committed if the domain refuses.
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
        let post = ctx.record(post);
        self.posts.save(&post, tx).await?;
        Ok(())
    }
}

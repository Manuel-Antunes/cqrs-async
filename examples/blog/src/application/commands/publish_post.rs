use cqrs::{
    aggregate::NotFound,
    command::{CommandContext, CommandHandler},
};

use crate::{
    application::{
        BlogError,
        commands::{PostCommandGateway, PostRepository},
    },
    domain::{PostEvent, PostId},
};

/// Publishes a draft; from then on the post is frozen.
#[cqrs::command(error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PublishPost {
    /// The post to publish.
    pub id: PostId,
}

/// Loads the post, lets the domain publish it and saves the result;
/// nothing is committed if the domain refuses.
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(&self, command: &PublishPost, ctx: &mut CommandContext<PostEvent, R::Tx>) -> Result<(), BlogError> {
        let post = self
            .posts
            .find(&command.id, ctx.tx())
            .await?
            .ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, ctx.tx()).await?;
        Ok(())
    }
}

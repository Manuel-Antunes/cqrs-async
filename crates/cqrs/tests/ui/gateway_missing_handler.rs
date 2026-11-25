use cqrs::{command::{CommandContext, CommandHandler}, store::StoreError};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::command(error = StoreError)]
struct Tick;

#[cqrs::command(error = StoreError)]
struct Ring;

#[cqrs::command_gateway(handles(Tick, Ring))]
struct Clock<P> {
    #[gateway(unit)]
    events: P,
}

#[cqrs::command_handler]
impl<P> CommandHandler<Tick> for Clock<P> {
    async fn handle(&self, _: &Tick, ctx: &mut CommandContext<Ticked>) -> Result<(), StoreError> {
        ctx.append(Ticked::Once);
        Ok(())
    }
}

fn main() {}

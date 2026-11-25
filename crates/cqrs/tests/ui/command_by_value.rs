use cqrs::{command::{CommandContext, CommandHandler}, store::StoreError};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::command(error = StoreError)]
struct Tick;

struct Clock;

#[cqrs::command_handler]
impl CommandHandler<Tick> for Clock {
    async fn handle(&self, _: Tick, _: &mut CommandContext<Ticked>) -> Result<(), StoreError> {
        Ok(())
    }
}

fn main() {}

#[cqrs::command(error = cqrs::store::StoreError)]
struct Tick;

#[cqrs::command_gateway(handles(Tick))]
struct Clock;

fn main() {}

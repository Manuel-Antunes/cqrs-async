#[cqrs::command(error = cqrs::store::StoreError)]
struct Tick;

#[cqrs::command_gateway(handles(Tick, Tick))]
struct Clock<P> {
    #[gateway(unit)]
    events: P,
}

fn main() {}

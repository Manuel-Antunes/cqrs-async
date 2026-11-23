//! Interceptors around a derived gateway: order, metadata, refusal, results.

use std::sync::Mutex;

use cqrs::{
    Command, CommandGateway, Handler, Intercepted, Interceptor, Message, Metadata, QueryGateway,
    testing::block_on,
};

#[derive(Debug, PartialEq)]
enum Error {
    Unauthorized,
}

#[derive(cqrs::Command)]
#[command(output = String, error = Error)]
struct Rename(String);

#[derive(cqrs::Query)]
#[query(output = u32, error = Error)]
struct Count;

/// Answers with the `user` it was sent by.
struct RenameHandler;

impl Handler for RenameHandler {
    type Message = Rename;

    async fn handle(&self, _: Rename) -> Result<String, Error> {
        unreachable!("always dispatched with metadata")
    }

    async fn handle_with(&self, Rename(name): Rename, metadata: Metadata) -> Result<String, Error> {
        Ok(format!("{name} by {}", metadata.get("user").map_or("nobody", String::as_str)))
    }
}

struct CountHandler;

impl Handler for CountHandler {
    type Message = Count;

    async fn handle(&self, _: Count) -> Result<u32, Error> {
        Ok(7)
    }
}

#[derive(cqrs::Gateway)]
struct Gateway {
    rename: RenameHandler,
    count: CountHandler,
}

/// Records what passes, before and after.
#[derive(Default)]
struct Journal(Mutex<Vec<String>>);

impl<M: Message> Interceptor<M> for Journal {
    async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send,
    {
        let name = std::any::type_name::<M>().rsplit("::").next().unwrap();
        self.0.lock().unwrap().push(format!("{name} in"));
        let result = next(message, metadata).await;
        self.0.lock().unwrap().push(format!("{name} {}", if result.is_ok() { "ok" } else { "failed" }));
        result
    }
}

/// Commands need a token; it becomes the `user`. Queries pass freely.
struct Authenticate;

impl<C: Command<Error = Error>> Interceptor<C> for Authenticate {
    async fn intercept<N, F>(&self, message: C, mut metadata: Metadata, next: N) -> Result<C::Output, Error>
    where
        N: FnOnce(C, Metadata) -> F + Send,
        F: Future<Output = Result<C::Output, Error>> + Send,
    {
        match metadata.remove("token").as_deref() {
            Some("secret") => {
                metadata.insert("user".into(), "ana".into());
                next(message, metadata).await
            }
            _ => Err(Error::Unauthorized),
        }
    }
}

fn token(value: &str) -> Metadata {
    Metadata::from([("token".to_owned(), value.to_owned())])
}

#[test]
fn interceptors_run_in_order_around_the_handler_and_can_refuse() {
    // A tuple runs in order: the journal sees every attempt, then
    // authentication refuses or lets it through to the second journal.
    let commands = Intercepted::new(
        Gateway { rename: RenameHandler, count: CountHandler },
        (Journal::default(), Authenticate, Journal::default()),
    );

    assert_eq!(block_on(commands.send_with(Rename("post".into()), token("secret"))), Ok("post by ana".into()));
    assert_eq!(block_on(commands.send_with(Rename("post".into()), token("wrong"))), Err(Error::Unauthorized));
    assert_eq!(block_on(commands.send(Rename("post".into()))), Err(Error::Unauthorized));

    let (outer, _, inner) = commands.interceptor();
    assert_eq!(*outer.0.lock().unwrap(), ["Rename in", "Rename ok", "Rename in", "Rename failed", "Rename in", "Rename failed"]);
    assert_eq!(*inner.0.lock().unwrap(), ["Rename in", "Rename ok"], "only what authentication let through");
}

#[test]
fn an_interceptor_for_every_message_wraps_queries_too() {
    let queries = Intercepted::new(Gateway { rename: RenameHandler, count: CountHandler }, Journal::default());
    assert_eq!(block_on(queries.query(Count)), Ok(7));
    fn is_query_gateway<G: QueryGateway<Count>>(_: &G) {}
    is_query_gateway(&queries);
    assert_eq!(*queries.interceptor().0.lock().unwrap(), ["Count in", "Count ok"]);
}

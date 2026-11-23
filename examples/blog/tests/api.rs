//! The GraphQL API over the asynchronous wiring ([`blog::start`]).
//!
//! Most tests execute against the schema directly; the last one goes through
//! a real socket: HTTP for queries and mutations, WebSocket
//! (`graphql-transport-ws`) for subscriptions.

use std::{sync::Arc, time::Duration};

use async_graphql::{Request, Response, Variables};
use blog::{
    Running,
    application::{commands::PostCommands, queries::{PostQueries, PostSubscriptions}},
    presentation::{
        BlogApi,
        graphql::{self, BlogSchema},
        http,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};

/// The running blog and its schema. The app's types are inferred by
/// `start`; the ports say what they are.
fn app() -> (
    Running<impl PostCommands, impl PostQueries + PostSubscriptions>,
    BlogSchema<impl BlogApi>,
) {
    let running = blog::start();
    let schema = graphql::schema(Arc::clone(&running.app));
    (running, schema)
}

async fn run(schema: &BlogSchema<impl BlogApi>, query: &str, variables: Value) -> Response {
    schema
        .execute(Request::new(query).variables(Variables::from_json(variables)))
        .await
}

/// Runs an operation that must succeed and returns its data.
async fn ok(schema: &BlogSchema<impl BlogApi>, query: &str, variables: Value) -> Value {
    let response = run(schema, query, variables).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}

/// Runs an operation that must fail and returns the first error's code.
async fn error_code(schema: &BlogSchema<impl BlogApi>, query: &str, variables: Value) -> String {
    let response = run(schema, query, variables).await;
    let error = response.errors.first().expect("an error");
    let code = error.extensions.as_ref().and_then(|e| e.get("code")).cloned();
    code.unwrap().into_json().unwrap().as_str().unwrap().to_owned()
}

async fn create(schema: &BlogSchema<impl BlogApi>, title: &str) -> String {
    let data = ok(
        schema,
        "mutation($title: String!) { createPost(title: $title) }",
        json!({ "title": title }),
    )
    .await;
    data["createPost"].as_str().unwrap().to_owned()
}

async fn publish(schema: &BlogSchema<impl BlogApi>, id: &str) {
    ok(
        schema,
        "mutation($id: ID!) { publishPost(id: $id) }",
        json!({ "id": id }),
    )
    .await;
}

/// Bounds every wait: a broken pipeline fails the test instead of hanging.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn mutations_commit_and_queries_see_them_once_projected() {
    let (running, schema) = app();
    let id = create(&schema, "Hello, async world").await;
    publish(&schema, &id).await;

    within(running.settled()).await; // the projection runs on its own task

    let data = ok(
        &schema,
        "query($id: ID!) { post(id: $id) { id title published } publishedPosts { id } }",
        json!({ "id": id }),
    )
    .await;
    assert_eq!(
        data,
        json!({
            "post": { "id": id, "title": "Hello, async world", "published": true },
            "publishedPosts": [{ "id": id }],
        })
    );
    assert_eq!(running.notifier.sent(), [format!("post-{id} published")]);
    running.stop().await;
}

#[tokio::test]
async fn errors_carry_a_code_clients_can_branch_on() {
    let (running, schema) = app();
    let create = "mutation($title: String!) { createPost(title: $title) }";
    let publish = "mutation($id: ID!) { publishPost(id: $id) }";

    assert_eq!(error_code(&schema, create, json!({ "title": "  " })).await, "DOMAIN_RULE");
    assert_eq!(error_code(&schema, publish, json!({ "id": "42" })).await, "NOT_FOUND");
    assert_eq!(error_code(&schema, publish, json!({ "id": "abc" })).await, "BAD_USER_INPUT");

    let id = self::create(&schema, "Once").await;
    self::publish(&schema, &id).await;
    assert_eq!(error_code(&schema, publish, json!({ "id": id })).await, "DOMAIN_RULE");
    running.stop().await;
}

#[tokio::test]
async fn post_events_replays_history_then_streams_new_events() {
    let (running, schema) = app();
    let first = create(&schema, "Before subscribing").await;

    let mut events = schema.execute_stream(Request::new(
        "subscription { postEvents { position postId kind title } }",
    ));
    let second = create(&schema, "After subscribing").await;
    publish(&schema, &second).await;

    let mut received = Vec::new();
    for _ in 0..3 {
        let response = within(events.next()).await.unwrap();
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        received.push(response.data.into_json().unwrap()["postEvents"].clone());
    }
    assert_eq!(
        received,
        [
            json!({ "position": "1", "postId": first, "kind": "CREATED", "title": "Before subscribing" }),
            json!({ "position": "2", "postId": second, "kind": "CREATED", "title": "After subscribing" }),
            json!({ "position": "3", "postId": second, "kind": "PUBLISHED", "title": null }),
        ]
    );
    running.stop().await;
}

#[tokio::test]
async fn post_events_resumes_after_a_known_position() {
    let (running, schema) = app();
    create(&schema, "a").await;
    create(&schema, "b").await;
    let mut events = schema.execute_stream(Request::new(
        r#"subscription { postEvents(after: "1") { position } }"#,
    ));
    let response = within(events.next()).await.unwrap();
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({ "postEvents": { "position": "2" } })
    );
    running.stop().await;
}

#[tokio::test]
async fn post_published_only_reports_new_publications() {
    let (running, schema) = app();
    let old = create(&schema, "Old news").await;
    publish(&schema, &old).await; // before subscribing: not reported

    let mut published = schema.execute_stream(Request::new("subscription { postPublished }"));
    // "From now on" is when the subscription first runs: let it start (the
    // test runtime is single-threaded, so one yield is enough) before acting.
    let next = tokio::spawn(async move { published.next().await });
    tokio::task::yield_now().await;

    create(&schema, "Draft").await; // created, not published: not reported
    let fresh = create(&schema, "Fresh").await;
    publish(&schema, &fresh).await;

    let response = within(next).await.unwrap().unwrap();
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({ "postPublished": fresh })
    );
    running.stop().await;
}

#[tokio::test]
async fn subscribers_are_independent_and_ordered_under_concurrent_writers() {
    let (running, schema) = app();
    let subscribe = || {
        schema.execute_stream(Request::new("subscription { postEvents { position } }"))
    };
    let (mut a, mut b) = (subscribe(), subscribe());

    // 20 posts created concurrently, from 20 tasks.
    let writers: Vec<_> = (0..20)
        .map(|n| {
            let schema = schema.clone();
            tokio::spawn(async move { create(&schema, &format!("post {n}")).await })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    for stream in [&mut a, &mut b] {
        let mut positions = Vec::new();
        for _ in 0..20 {
            let response = within(stream.next()).await.unwrap();
            let data = response.data.into_json().unwrap();
            positions.push(data["postEvents"]["position"].as_str().unwrap().parse::<u64>().unwrap());
        }
        assert_eq!(positions, (1..=20).collect::<Vec<_>>()); // every event, once, in order
    }
    running.stop().await;
}

fn subscribe<A: BlogApi>(
    schema: &BlogSchema<A>,
    query: &str,
    variables: Value,
) -> impl futures_util::Stream<Item = Response> + use<A> {
    schema.execute_stream(Request::new(query).variables(Variables::from_json(variables)))
}

/// The next pushed result's data, failing on errors.
async fn next_data(stream: &mut (impl futures_util::Stream<Item = Response> + Unpin)) -> Value {
    let response = within(stream.next()).await.expect("the subscription ended");
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}

/// Nothing is pushed for a while.
async fn quiet(stream: &mut (impl futures_util::Stream<Item = Response> + Unpin)) {
    if let Ok(Some(response)) = tokio::time::timeout(Duration::from_millis(300), stream.next()).await {
        panic!("unexpected push: {:?}", response.data);
    }
}

async fn retitle(schema: &BlogSchema<impl BlogApi>, id: &str, title: &str) {
    ok(
        schema,
        "mutation($id: ID!, $title: String!) { retitlePost(id: $id, title: $title) }",
        json!({ "id": id, "title": title }),
    )
    .await;
}

#[tokio::test]
async fn subscribing_to_a_post_has_the_query_shape_and_follows_its_changes() {
    let (running, schema) = app();
    let id = create(&schema, "Draft").await;
    let other = create(&schema, "Other").await;

    let mut post = Box::pin(subscribe(
        &schema,
        "subscription($id: ID!) { post(id: $id) { id title published } }",
        json!({ "id": id }),
    ));
    // The current state first, already projected.
    assert_eq!(
        next_data(&mut post).await,
        json!({ "post": { "id": id, "title": "Draft", "published": false } })
    );

    retitle(&schema, &id, "Final").await;
    assert_eq!(next_data(&mut post).await["post"]["title"], "Final");
    publish(&schema, &id).await;
    assert_eq!(next_data(&mut post).await["post"]["published"], true);

    retitle(&schema, &other, "Unrelated").await; // another post: filtered out
    quiet(&mut post).await;
    running.stop().await;
}

#[tokio::test]
async fn subscribing_to_a_post_that_does_not_exist_yet_starts_with_null() {
    let (running, schema) = app();
    let mut post = Box::pin(subscribe(&schema, r#"subscription { post(id: "1") { title } }"#, json!({})));
    assert_eq!(next_data(&mut post).await, json!({ "post": null }));
    create(&schema, "Born later").await;
    assert_eq!(next_data(&mut post).await, json!({ "post": { "title": "Born later" } }));
    running.stop().await;
}

#[tokio::test]
async fn the_published_list_ignores_drafts_and_updates_on_publication() {
    let (running, schema) = app();
    let mut published = Box::pin(subscribe(&schema, "subscription { publishedPosts { id title } }", json!({})));
    assert_eq!(next_data(&mut published).await, json!({ "publishedPosts": [] }));

    let id = create(&schema, "Soon").await; // a draft: not refreshed at all
    quiet(&mut published).await;

    publish(&schema, &id).await;
    assert_eq!(
        next_data(&mut published).await,
        json!({ "publishedPosts": [{ "id": id, "title": "Soon" }] })
    );
    running.stop().await;
}

#[tokio::test]
async fn post_events_are_filtered_on_the_server_by_post_and_kind() {
    let (running, schema) = app();
    let a = create(&schema, "a").await; // 1
    let b = create(&schema, "b").await; // 2
    publish(&schema, &b).await; // 3
    retitle(&schema, &a, "a2").await; // 4

    let query = "subscription($post: ID, $kinds: [PostEventKind!], $after: ID) {
        postEvents(postId: $post, kinds: $kinds, after: $after) { position postId kind }
    }";
    let events = |vars: Value| Box::pin(subscribe(&schema, query, vars));

    // By post.
    let mut of_a = events(json!({ "post": a }));
    for position in ["1", "4"] {
        let event = next_data(&mut of_a).await;
        assert_eq!(event["postEvents"]["position"], position);
        assert_eq!(event["postEvents"]["postId"], a.as_str());
    }
    quiet(&mut of_a).await;

    // By kind.
    let mut published = events(json!({ "kinds": ["PUBLISHED"] }));
    assert_eq!(next_data(&mut published).await["postEvents"]["position"], "3");
    quiet(&mut published).await;

    // Both, resumed after a position, then live.
    let mut of_a_changes = events(json!({ "post": a, "kinds": ["PUBLISHED", "RETITLED"], "after": "1" }));
    assert_eq!(next_data(&mut of_a_changes).await["postEvents"]["position"], "4");
    create(&schema, "c").await; // 5: another post
    publish(&schema, &a).await; // 6: matches both filters
    assert_eq!(
        next_data(&mut of_a_changes).await["postEvents"],
        json!({ "position": "6", "postId": a, "kind": "PUBLISHED" })
    );
    assert_eq!(next_data(&mut published).await["postEvents"]["position"], "6");
    running.stop().await;
}

#[tokio::test]
async fn invalid_filters_fail_the_subscription_with_a_code() {
    let (running, schema) = app();
    for query in [
        r#"subscription { postEvents(postId: "abc") { position } }"#,
        r#"subscription { postEvents(after: "-1") { position } }"#,
        r#"subscription { post(id: "x") { id } }"#,
    ] {
        let response = within(Box::pin(subscribe(&schema, query, json!({}))).next()).await.unwrap();
        let code = response.errors[0].extensions.as_ref().and_then(|e| e.get("code")).cloned();
        assert_eq!(code.unwrap().into_json().unwrap(), "BAD_USER_INPUT", "{query}");
    }
    running.stop().await;
}

#[tokio::test]
async fn over_http_and_websocket() {
    let (running, schema) = app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, http::router(schema)).await.unwrap();
    });

    // Subscribe over WebSocket.
    let mut request = format!("ws://{address}{}", http::SUBSCRIPTIONS_PATH)
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static("graphql-transport-ws"),
    );
    let (mut socket, _) = within(tokio_tungstenite::connect_async(request)).await.unwrap();
    let send = |value: Value| Message::text(value.to_string());
    socket.send(send(json!({ "type": "connection_init" }))).await.unwrap();
    let ack = within(next_json(&mut socket)).await;
    assert_eq!(ack["type"], "connection_ack");
    for (id, query) in [
        ("events", "subscription { postEvents { postId kind title } }"),
        ("published", "subscription { postEvents(kinds: [PUBLISHED]) { postId kind } }"),
    ] {
        socket
            .send(send(json!({ "id": id, "type": "subscribe", "payload": { "query": query } })))
            .await
            .unwrap();
    }

    // Change things over plain HTTP.
    let id = post_json(
        address,
        json!({ "query": r#"mutation { createPost(title: "Over the wire") }"# }),
    )
    .await["data"]["createPost"]
        .as_str()
        .unwrap()
        .to_owned();
    post_json(
        address,
        json!({ "query": "mutation($id: ID!) { publishPost(id: $id) }", "variables": { "id": id } }),
    )
    .await;

    // Both changes arrive on the unfiltered subscription, in order; only
    // the publication on the filtered one (same socket, told apart by id).
    let (mut all, mut published) = (Vec::new(), Vec::new());
    while all.len() < 2 || published.is_empty() {
        let message = within(next_json(&mut socket)).await;
        assert_eq!(message["type"], "next", "{message}");
        let event = &message["payload"]["data"]["postEvents"];
        assert_eq!(event["postId"], id.as_str());
        let kind = event["kind"].as_str().unwrap().to_owned();
        match message["id"].as_str().unwrap() {
            "events" => all.push(kind),
            _ => published.push(kind),
        }
    }
    assert_eq!(all, ["CREATED", "PUBLISHED"]);
    assert_eq!(published, ["PUBLISHED"]);

    // And the read model answers over HTTP once projected.
    within(running.settled()).await;
    let post = post_json(
        address,
        json!({ "query": "query($id: ID!) { post(id: $id) { title published } }", "variables": { "id": id } }),
    )
    .await;
    assert_eq!(
        post["data"]["post"],
        json!({ "title": "Over the wire", "published": true })
    );

    // GraphiQL is served for browsers.
    let page = raw_http(address, "GET /graphql HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.contains("graphiql"));

    server.abort();
    running.stop().await;
}

async fn next_json<S>(socket: &mut S) -> Value
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match socket.next().await.expect("socket closed").unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected message {other:?}"),
        }
    }
}

/// POSTs a GraphQL request with nothing but a TCP socket.
async fn post_json(address: std::net::SocketAddr, body: Value) -> Value {
    post_json_with(address, body, "").await
}

/// [`post_json`] with extra header lines (each ending in `\r\n`).
async fn post_json_with(address: std::net::SocketAddr, body: Value, headers: &str) -> Value {
    let body = body.to_string();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",
        http::GRAPHQL_PATH,
        body.len(),
    );
    let response = raw_http(address, &request).await;
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    serde_json::from_str(body).unwrap()
}

async fn raw_http(address: std::net::SocketAddr, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    within(stream.read_to_string(&mut response)).await.unwrap();
    response
}

#[tokio::test]
async fn the_request_correlation_id_is_stored_with_the_events_it_causes() {
    let (running, schema) = app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, http::router(schema)).await.unwrap();
    });

    let body = json!({ "query": "mutation { createPost(title: \"Traced\") }" });
    let response = post_json_with(address, body, "x-correlation-id: req-7\r\n").await;
    assert!(response.get("errors").is_none(), "{response}");

    let stored = running.app.events().store().all();
    let correlation: Vec<_> = stored.iter().map(|event| event.metadata.get("correlation_id").cloned()).collect();
    assert_eq!(correlation, [Some("req-7".to_owned())]);
    server.abort();
    running.stop().await;
}

#[tokio::test]
async fn interceptors_audit_every_command_and_measure_the_projection() {
    let (running, schema) = app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, http::router(schema)).await.unwrap();
    });

    let body = json!({ "query": "mutation { createPost(title: \"Audited\") }" });
    let created = post_json_with(address, body, "x-correlation-id: req-9\r\n").await;
    let id = created["data"]["createPost"].as_str().unwrap().to_owned();
    // Refused by the domain (already published twice): audited as failed.
    let publish = json!({ "query": format!("mutation {{ publishPost(id: \"{id}\") }}") });
    post_json_with(address, publish.clone(), "").await;
    post_json_with(address, publish, "").await;
    running.settled().await;

    let entries = running.audit.entries();
    let summary: Vec<_> = entries.iter().map(|e| (e.command, e.correlation_id.as_deref(), e.succeeded)).collect();
    assert_eq!(summary, [("CreatePost", Some("req-9"), true), ("PublishPost", None, true), ("PublishPost", None, false)]);
    assert_eq!(running.metrics.handled(), 2, "created and published, each applied once");
    server.abort();
    running.stop().await;
}

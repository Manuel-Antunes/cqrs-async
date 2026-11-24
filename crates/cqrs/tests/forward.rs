//! The log as an outbox: a tracking processor running [`Forward`] carries
//! every committed event to a broker, encoded by a codec, keyed by its
//! aggregate, retrying while the broker is down.

#![cfg(feature = "tokio")]

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use cqrs::{
    AppendEvents, Codec, CodecError, Events, ExpectedVersion, Forward, InMemoryEventStore, Metadata, Outgoing, Publish,
    StreamId,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
enum Shipped {
    #[event(name = "parcel.shipped")]
    Parcel(u32),
}

/// A format of the application's own: `parcel:<n>`.
struct Plain;

impl Codec<Shipped> for Plain {
    fn encode(&self, Shipped::Parcel(n): &Shipped) -> Result<Vec<u8>, CodecError> {
        Ok(format!("parcel:{n}").into_bytes())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Shipped, CodecError> {
        let text = std::str::from_utf8(bytes).map_err(CodecError::new)?;
        let n = text
            .strip_prefix("parcel:")
            .ok_or_else(|| CodecError::new("not a parcel"))?;
        Ok(Shipped::Parcel(n.parse().map_err(CodecError::new)?))
    }
}

#[derive(Debug)]
struct Down;

impl fmt::Display for Down {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the broker is down")
    }
}

impl std::error::Error for Down {}

/// What the broker received: topic, key, payload, position.
type Sent = (String, String, String, u64);

/// A broker that refuses the first `failures` messages, then records.
#[derive(Clone, Default)]
struct Broker {
    failures: Arc<Mutex<u32>>,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Publish for Broker {
    type Error = Down;

    async fn publish(&self, message: Outgoing<'_>) -> Result<(), Down> {
        {
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(Down);
            }
        }
        self.sent.lock().unwrap().push((
            message.topic.to_owned(),
            message.key.to_owned(),
            String::from_utf8(message.payload.to_vec()).unwrap(),
            message.position,
        ));
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn committed_events_reach_the_broker_in_order_even_after_it_was_down() {
    let events = Events::new(InMemoryEventStore::new());
    let broker = Broker::default();
    *broker.failures.lock().unwrap() = 2;
    let outbox = events.track("outbox", Forward::new(broker.clone(), Plain, "parcels"));

    let stream = StreamId::new("parcel", "7");
    let committed = events
        .store()
        .append(
            &mut (),
            &stream,
            ExpectedVersion::NoStream,
            vec![Shipped::Parcel(1), Shipped::Parcel(2)],
            &Metadata::new(),
        )
        .await
        .unwrap();
    outbox.caught_up(committed.last().unwrap().position).await;
    outbox.stop().await;

    assert_eq!(
        *broker.sent.lock().unwrap(),
        [
            ("parcels".to_owned(), "parcel-7".to_owned(), "parcel:1".to_owned(), 1),
            ("parcels".to_owned(), "parcel-7".to_owned(), "parcel:2".to_owned(), 2),
        ]
    );
    assert_eq!(Plain.decode(b"parcel:2").unwrap(), Shipped::Parcel(2));
}

#[tokio::test(start_paused = true)]
async fn a_routed_forwarder_picks_the_topic_per_event() {
    let events = Events::new(InMemoryEventStore::new());
    let broker = Broker::default();
    let forward = Forward::routed(broker.clone(), Plain, |event: &cqrs::EventEnvelope<Shipped>| {
        event.event_type.replace('.', "-")
    });
    let outbox = events.track("outbox", forward);
    let committed = events
        .store()
        .append(
            &mut (),
            &StreamId::new("parcel", "1"),
            ExpectedVersion::NoStream,
            vec![Shipped::Parcel(9)],
            &Metadata::new(),
        )
        .await
        .unwrap();
    outbox.caught_up(committed[0].position).await;
    outbox.stop().await;
    assert_eq!(broker.sent.lock().unwrap()[0].0, "parcel-shipped");
}

//! Dynamic consistency boundaries (Axon 5's DCB), with the classic example:
//! a course has a capacity and a student a limit of courses. Both are
//! aggregate roots built from tags (`#[aggregate(tag = ...)]`), so a
//! subscription, tagged with both, counts for both, and a decision is
//! refused if anything either was built from changed since it was loaded.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use cqrs::{
    Aggregate, AggregateLoader, AppendEvents, Appender, CommandGateway, CommandHandler, EventAppender, ExpectedVersion,
    InMemoryEventStore, InlinePublisher, Metadata, SourceEvents, StoreError, StreamId, TagResolver, testing::block_on,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
enum SchoolEvent {
    #[event(name = "course.opened")]
    CourseOpened {
        #[tag]
        course: u32,
        capacity: usize,
    },
    #[event(name = "student.subscribed")]
    Subscribed {
        #[tag]
        course: u32,
        #[tag]
        student: u32,
    },
}

/// Every event tagged with its id: its opening and its subscriptions.
#[derive(Debug, cqrs::AggregateRoot)]
#[aggregate(event = SchoolEvent, tag = "course")]
struct Course {
    id: u32,
    capacity: usize,
    students: Vec<u32>,
}

impl Course {
    fn apply(&mut self, event: &SchoolEvent) {
        match event {
            SchoolEvent::CourseOpened { capacity, .. } => self.capacity = *capacity,
            SchoolEvent::Subscribed { student, .. } => self.students.push(*student),
        }
    }

    /// Subscribes `student`, whose aggregate is `None` until its first
    /// subscription.
    fn subscribe(
        &self,
        id: u32,
        student: Option<&Student>,
        events: &mut impl EventAppender<SchoolEvent>,
    ) -> Result<(), Error> {
        if self.students.len() >= self.capacity {
            return Err(Error::Full);
        }
        if student.is_some_and(|student| student.courses >= 2) {
            return Err(Error::TooManyCourses);
        }
        events.append(SchoolEvent::Subscribed {
            course: self.id,
            student: id,
        });
        Ok(())
    }
}

/// Every subscription of the student, whichever course's stream holds it.
#[derive(Debug, cqrs::AggregateRoot)]
#[aggregate(event = SchoolEvent, tag = "student")]
struct Student {
    #[aggregate(id)]
    id: u32,
    courses: usize,
}

impl Student {
    fn apply(&mut self, event: &SchoolEvent) {
        if let SchoolEvent::Subscribed { .. } = event {
            self.courses += 1;
        }
    }
}

#[derive(Debug)]
enum Error {
    NoSuchCourse,
    Full,
    TooManyCourses,
    Store(StoreError),
}

/// Equal kinds; store errors by their message (they keep their cause, which
/// is not comparable).
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Store(a), Self::Store(b)) => a.to_string() == b.to_string(),
            (Self::NoSuchCourse, Self::NoSuchCourse)
            | (Self::Full, Self::Full)
            | (Self::TooManyCourses, Self::TooManyCourses) => true,
            _ => false,
        }
    }
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[derive(cqrs::Command)]
#[command(error = Error)]
struct Subscribe {
    student: u32,
    courses: Vec<u32>,
}

type Store = InMemoryEventStore<SchoolEvent>;
type Publisher = Arc<InlinePublisher<SchoolEvent, Store>>;

/// Subscribes to each course in turn. On its first run it can let another
/// student take a seat between its loads and its save.
struct SubscribeHandler {
    courses: Arc<AggregateLoader<Course, Store>>,
    students: Arc<AggregateLoader<Student, Store>>,
    events: Publisher,
    runs: AtomicU32,
    sneak_in: Option<u32>,
}

#[cqrs::command_handler]
impl CommandHandler for SubscribeHandler {
    async fn handle(&self, command: &Subscribe, events: &mut Appender<SchoolEvent>) -> Result<(), Error> {
        let first_run = self.runs.fetch_add(1, Ordering::Relaxed) == 0;
        for &course in &command.courses {
            let course = self
                .courses
                .load_in(&course, events)
                .await?
                .ok_or(Error::NoSuchCourse)?;
            // Read, not saved: still part of what the save checks.
            let student = self.students.load_in(&command.student, events).await?;
            if let (Some(other), true) = (self.sneak_in, first_run) {
                subscribe_directly(self.courses.store(), course.id, other);
            }
            course.subscribe(command.student, student.as_deref(), events)?;
            events.commit(course, &self.events).await?;
        }
        Ok(())
    }
}

#[derive(cqrs::Gateway)]
#[gateway(commands)]
struct Gateway {
    subscribe: SubscribeHandler,
}

/// The gateway, committing through an engine over the same store.
/// The gateway and the publisher its handler commits through.
struct Wired {
    gateway: Gateway,
    publisher: Publisher,
}

impl Wired {
    fn inner(&self) -> &Gateway {
        &self.gateway
    }

    fn publisher(&self) -> &Publisher {
        &self.publisher
    }
}

impl std::ops::Deref for Wired {
    type Target = Gateway;

    fn deref(&self) -> &Gateway {
        &self.gateway
    }
}

fn gateway(sneak_in: Option<u32>) -> (Store, Wired) {
    let store: Store = InMemoryEventStore::new();
    for (course, capacity) in [(1, 1), (2, 5), (3, 5)] {
        let opened = SchoolEvent::CourseOpened { course, capacity };
        block_on(store.append(
            &mut (),
            &StreamId::of::<Course>(&course),
            ExpectedVersion::NoStream,
            vec![opened],
            &Metadata::new(),
        ))
        .unwrap();
    }
    let publisher: Publisher = Arc::new(InlinePublisher::new(store.clone()));
    let handler = SubscribeHandler {
        events: Arc::clone(&publisher),
        courses: Arc::new(AggregateLoader::new(store.clone())),
        students: Arc::new(AggregateLoader::new(store.clone())),
        runs: AtomicU32::new(0),
        sneak_in,
    };
    (
        store,
        Wired {
            gateway: Gateway { subscribe: handler },
            publisher,
        },
    )
}

/// Another writer's subscription, straight into the store.
fn subscribe_directly(store: &Store, course: u32, student: u32) {
    let event = SchoolEvent::Subscribed { course, student };
    block_on(store.append(
        &mut (),
        &StreamId::of::<Course>(&course),
        ExpectedVersion::Any,
        vec![event],
        &Metadata::new(),
    ))
    .unwrap();
}

#[test]
fn the_derive_builds_the_aggregate_from_its_tag() {
    assert_eq!(Course::TYPE, "course");
    assert_eq!(Course::criteria(&7), Some(cqrs::criteria![course = 7]));
    let student = Student::initial(&3);
    assert_eq!((student.id, student.courses), (3, 0));
    assert_eq!(
        SchoolEvent::Subscribed { course: 1, student: 2 }.tags(),
        cqrs::tags![course = 1, student = 2]
    );
}

#[test]
fn rules_spanning_streams_are_enforced() {
    let (_, gateway) = gateway(None);
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 1,
            courses: vec![1]
        })),
        Ok(())
    );
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 2,
            courses: vec![1]
        })),
        Err(Error::Full)
    );
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 1,
            courses: vec![2]
        })),
        Ok(())
    );
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 1,
            courses: vec![3]
        })),
        Err(Error::TooManyCourses)
    );
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 9,
            courses: vec![4]
        })),
        Err(Error::NoSuchCourse)
    );
}

#[test]
fn a_decision_on_stale_loads_is_refused_and_decided_again() {
    // Student 2 takes the last seat of course 1 between student 1's loads
    // and save; only the condition can catch it (no stream version is
    // involved).
    let (store, gateway) = gateway(Some(2));
    assert_eq!(
        block_on(gateway.send(Subscribe {
            student: 1,
            courses: vec![1]
        })),
        Err(Error::Full)
    );
    assert_eq!(
        gateway.inner().subscribe.runs.load(Ordering::Relaxed),
        2,
        "refused once, then decided on fresh loads"
    );
    let subscribed = store
        .all()
        .into_iter()
        .filter(|e| matches!(e.payload, SchoolEvent::Subscribed { .. }))
        .count();
    assert_eq!(subscribed, 1, "only student 2");
}

#[test]
fn a_read_only_aggregate_is_checked_too() {
    // Student 1 subscribes elsewhere while course 2 is being decided: the
    // course did not change, the student did, and the student was loaded.
    let (store, gateway) = gateway(None);
    let course = block_on(gateway.inner().subscribe.courses.load(&2)).unwrap().unwrap();
    let mut events = Appender::with_metadata(Metadata::new());
    let student = block_on(gateway.inner().subscribe.students.load_in(&1, &mut events)).unwrap();
    assert!(student.is_none(), "no subscription yet: absence is checked too");
    subscribe_directly(&store, 3, 1);
    course.subscribe(1, None, &mut events).unwrap();
    let refused = block_on(events.commit(course, gateway.publisher()));
    assert!(matches!(refused, Err(StoreError::ConditionFailed)));
}

#[test]
fn an_aggregate_saved_twice_checks_only_what_others_append() {
    let (_, subscriber) = gateway(None);
    assert_eq!(
        block_on(subscriber.send(Subscribe {
            student: 1,
            courses: vec![2, 3]
        })),
        Ok(())
    );

    // The same aggregate saved twice: its own first events do not refuse
    // the second save.
    let (_, other) = gateway(None);
    let courses = &other.inner().subscribe.courses;
    let mut events = Appender::with_metadata(Metadata::new());
    let course = block_on(courses.load_in(&2, &mut events)).unwrap().unwrap();
    events.append(SchoolEvent::Subscribed { course: 2, student: 5 });
    block_on(events.commit(course, other.publisher())).unwrap();
    let course = block_on(courses.load_in(&2, &mut events)).unwrap().unwrap();
    assert_eq!(course.students, [5]);
    events.append(SchoolEvent::Subscribed { course: 2, student: 6 });
    block_on(events.commit(course, other.publisher())).unwrap();
    assert_eq!(block_on(courses.load(&2)).unwrap().unwrap().students, [5, 6]);
}

#[test]
fn a_new_aggregate_root_is_created_in_its_own_stream() {
    let (store, gateway) = gateway(None);
    let course = cqrs::New::<Course>(9);
    let mut events = Appender::with_metadata(Metadata::new());
    events.append(SchoolEvent::CourseOpened { course: 9, capacity: 3 });
    block_on(events.commit(course, gateway.publisher())).unwrap();
    let loaded = block_on(gateway.inner().subscribe.courses.load(&9)).unwrap().unwrap();
    assert_eq!(loaded.capacity, 3);
    assert_eq!(store.all().last().unwrap().stream, StreamId::of::<Course>(&9));
}

/// Tags written by hand (an `impl TagResolver`, no `#[tag]` attributes on
/// the event): criteria and conditions work on them the same way.
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Shipping {
    Shipped { parcel: u32, carrier: String },
}

impl TagResolver for Shipping {
    fn tags(&self) -> Vec<cqrs::Tag> {
        let Self::Shipped { carrier, .. } = self;
        vec![cqrs::Tag::new("carrier", carrier)]
    }
}

#[test]
fn hand_written_tags_select_and_guard_like_derived_ones() {
    let store = InMemoryEventStore::new();
    let ship = |parcel: u32, carrier: &str, condition: Option<&cqrs::AppendCondition>| {
        let event = Shipping::Shipped {
            parcel,
            carrier: carrier.to_owned(),
        };
        let stream = StreamId::new("parcel", parcel.to_string());
        block_on(store.append_if(
            &mut (),
            &stream,
            ExpectedVersion::Any,
            vec![event],
            &Metadata::new(),
            condition,
        ))
    };
    ship(1, "dhl", None).unwrap();
    ship(2, "ups", None).unwrap();

    let dhl = cqrs::criteria![carrier = "dhl"];
    let (events, head) = block_on(store.source(&dhl)).unwrap();
    assert_eq!(events.iter().map(|e| e.position).collect::<Vec<_>>(), [1]);
    let condition = cqrs::AppendCondition {
        criteria: dhl,
        after: head,
    };
    ship(3, "ups", None).unwrap();
    assert!(
        ship(4, "dhl", Some(&condition)).is_ok(),
        "only a dhl parcel since would refuse it"
    );
    assert!(matches!(
        ship(5, "dhl", Some(&condition)),
        Err(StoreError::ConditionFailed)
    ));
    // Nothing to append succeeds at once, whatever the condition (every store).
    let (stream, metadata) = (StreamId::new("parcel", "6"), Metadata::new());
    let nothing = block_on(store.append_if(
        &mut (),
        &stream,
        ExpectedVersion::Exact(9),
        vec![],
        &metadata,
        Some(&condition),
    ));
    assert!(nothing.unwrap().is_empty());
}

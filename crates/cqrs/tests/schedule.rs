//! Commands sent later: a handler schedules within its unit of work, and a
//! deliverer sends what is due through the gateway; a cancelled command is
//! never sent, a failing one is tried again, then given up.

#![cfg(feature = "tokio")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime},
};

use cqrs::{
    command::{CommandContext, CommandGateway, CommandHandler, DeliveryOptions, InMemorySchedule, deliver_scheduled},
    event::Metadata,
    processing::InlinePublisher,
    store::{InMemoryEventStore, ReadLog, StoreError},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum TaskEvent {
    Opened,
    Reminded,
}

#[cqrs::aggregate(name = "task", event = TaskEvent)]
struct Task {
    id: u32,
}

impl Task {
    fn apply(&mut self, _: &TaskEvent) {}
}

#[derive(Debug, PartialEq)]
enum TaskError {
    Refused,
    Store(String),
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<StoreError> for TaskError {
    fn from(error: StoreError) -> Self {
        Self::Store(error.to_string())
    }
}

/// Opens a task, and schedules its reminder.
#[cqrs::command(error = TaskError)]
struct Open {
    id: u32,
    remind_in: Duration,
}

/// Reminds about a task; refused for task 13.
#[cqrs::command(error = TaskError)]
#[derive(Clone, Debug)]
struct Remind {
    id: u32,
}

type Store = InMemoryEventStore<TaskEvent>;

#[cqrs::command_gateway(handles(Open, Remind))]
struct Tasks {
    reminders: InMemorySchedule<Remind>,
    refused: AtomicU32,
    #[gateway(unit)]
    events: InlinePublisher<TaskEvent, Store>,
}

#[cqrs::command_handler]
impl CommandHandler<Open> for Tasks {
    async fn handle(&self, command: &Open, ctx: &mut CommandContext<TaskEvent>, tx: &mut ()) -> Result<(), TaskError> {
        use cqrs::command::CommandSchedule;
        ctx.append(TaskEvent::Opened);
        ctx.record(cqrs::aggregate::New::<Task>(command.id));
        // In the unit of work: scheduled once the events commit.
        let due = SystemTime::now() + command.remind_in;
        self.reminders
            .schedule_in(tx, due, &Remind { id: command.id }, ctx.metadata())
            .await?;
        Ok(())
    }
}

#[cqrs::command_handler]
impl CommandHandler<Remind> for Tasks {
    async fn handle(&self, command: &Remind, ctx: &mut CommandContext<TaskEvent>) -> Result<(), TaskError> {
        if command.id == 13 {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(TaskError::Refused);
        }
        ctx.append(TaskEvent::Reminded);
        ctx.record(cqrs::aggregate::Loaded::at(Task { id: command.id }, 1));
        Ok(())
    }
}

async fn reminded(store: &Store) -> usize {
    store
        .read_all(0, 100)
        .await
        .unwrap()
        .iter()
        .filter(|event| event.payload == TaskEvent::Reminded)
        .count()
}

#[tokio::test]
async fn scheduled_commands_are_sent_when_due_and_failures_given_up() {
    let store = Store::new();
    let reminders = InMemorySchedule::default();
    let tasks = Arc::new(Tasks {
        reminders: reminders.clone(),
        refused: AtomicU32::new(0),
        events: InlinePublisher::new(store.clone()),
    });
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let options = DeliveryOptions::default()
        .poll_interval(Duration::from_millis(10))
        .max_attempts(2)
        .retry_initial(Duration::from_millis(10));
    let delivering = tokio::spawn(deliver_scheduled(
        reminders.clone(),
        Arc::clone(&tasks),
        options,
        async {
            let _ = stopped.await;
        },
    ));

    tasks
        .send(Open {
            id: 1,
            remind_in: Duration::ZERO,
        })
        .await
        .unwrap();
    tasks
        .send(Open {
            id: 2,
            remind_in: Duration::from_millis(400),
        })
        .await
        .unwrap();
    tasks
        .send(Open {
            id: 13,
            remind_in: Duration::ZERO,
        })
        .await
        .unwrap();
    // A cancelled reminder is never sent.
    let cancelled = reminders.schedule(SystemTime::now(), Remind { id: 3 }, Metadata::new());
    use cqrs::command::CommandSchedule;
    assert!(reminders.cancel(cancelled).await.unwrap());

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(reminded(&store).await, 1, "task 1 is due, task 2 not yet");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(reminded(&store).await, 2, "task 2 came due");
    assert_eq!(tasks.refused.load(Ordering::SeqCst), 2, "task 13: tried twice");
    let left = reminders.pending();
    assert_eq!(left.len(), 1, "only the given-up command is kept");
    assert_eq!(left[0].1.id, 13);
    assert_eq!(left[0].2.as_deref(), Some("Refused"));

    let _ = stop.send(());
    delivering.await.unwrap();
}

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

use discordy_messages_application::ports::scheduled::ScheduleKey;
use discordy_messages_application::{
    fakes::{ALICE, CHANNEL, Harness, START_MS},
    ports::Clock,
    scheduled::{CreateSchedule, ScheduleAddress, ScheduledPorts},
    use_cases::CreateMessage,
};
use discordy_messages_domain::scheduled::{MIN_SCHEDULE_DELAY_MS, ScheduledDraft};
use discordy_messages_infrastructure::{
    scylla::{ScyllaMessageRepository, ScyllaScheduledMessages},
    system::{RandomScheduleAttempts, SystemClock},
};

use super::{faults::ObservedMessages, support};

pub struct Fixture {
    pub _database: support::Scylla,
    pub harness: Harness,
    pub messages: Arc<ObservedMessages>,
    pub first: ScheduledPorts,
    pub second: ScheduledPorts,
}

impl Fixture {
    pub async fn new(keyspace: &str) -> Self {
        let database = support::start_scylla(keyspace).await;
        let mut harness = Harness::new();
        harness
            .clock
            .advance_ms(SystemClock.now_unix_ms().saturating_sub(START_MS));
        let messages = Arc::new(ObservedMessages {
            inner: Arc::new(
                ScyllaMessageRepository::for_scheduled_delivery(database.session.clone())
                    .await
                    .unwrap(),
            ),
            calls: AtomicUsize::new(0),
            lose_result: AtomicBool::new(false),
        });
        harness.ports.messages = messages.clone();
        let delivery = Arc::new(CreateMessage::new(&harness.ports));
        let a = Arc::new(
            ScyllaScheduledMessages::open(database.session.clone(), None)
                .await
                .unwrap(),
        );
        let b = Arc::new(
            ScyllaScheduledMessages::open(database.session.clone(), None)
                .await
                .unwrap(),
        );
        let first = ScheduledPorts {
            journal: a.clone(),
            index: a,
            delivery: delivery.clone(),
            attempts: Arc::new(RandomScheduleAttempts),
            clock: harness.clock.clone(),
        };
        let second = ScheduledPorts {
            journal: b.clone(),
            index: b,
            delivery,
            attempts: Arc::new(RandomScheduleAttempts),
            clock: harness.clock.clone(),
        };
        Self {
            _database: database,
            harness,
            messages,
            first,
            second,
        }
    }

    pub fn command(&self, sequence: u64) -> CreateSchedule {
        CreateSchedule {
            id: format!("00000000-0000-4000-8000-{:012x}", sequence * 64 + ALICE.value() % 64)
                .parse()
                .unwrap(),
            author_id: ALICE,
            channel_id: CHANNEL,
            scheduled_at_ms: self.harness.clock.now_unix_ms() + MIN_SCHEDULE_DELAY_MS,
            draft: ScheduledDraft {
                content: "scheduled through real Scylla".into(),
                tts: false,
                reply_to: None,
                replied_user: true,
                silent: false,
            },
        }
    }
}

pub fn address(command: &CreateSchedule) -> ScheduleAddress {
    ScheduleAddress {
        key: ScheduleKey {
            author_id: command.author_id,
            id: command.id,
        },
        channel_id: command.channel_id,
    }
}

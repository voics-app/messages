//! A notification sent by hand: the owner picks one user or everyone in the admin portal, the
//! admin service calls the notifier, and the push goes through the same bounded queue and
//! in-flight cap as every message push (ADR 0016). Nothing here talks to a provider directly.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use super::dispatcher::{Dispatcher, Submitted};
use super::ports::{DeviceDirectory, DeviceToken, PortError};
use crate::domain::{AlertContent, OutboundPush};

pub const MAX_TITLE_CHARS: usize = 64;
pub const MAX_BODY_CHARS: usize = 240;
const MAX_ACTOR_CHARS: usize = 200;
/// A broadcast stops here so one request cannot run for ever; the report says so.
pub const MAX_BROADCAST_DEVICES: usize = 50_000;
/// How long one push may wait for room in the queue before it is dropped and counted.
const QUEUE_PATIENCE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualTarget {
    User(String),
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRequest {
    pub target: ManualTarget,
    pub title: String,
    pub body: String,
    /// Only count the devices that would receive it; send nothing.
    pub dry_run: bool,
    /// Who asked (the admin's email), for the log only.
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ManualReport {
    /// Devices the notification goes to.
    pub devices: usize,
    /// Of those, handed to the queue.
    pub queued: usize,
    /// Of those, dropped because the queue stayed full.
    pub dropped: usize,
    /// The broadcast hit [`MAX_BROADCAST_DEVICES`] and the rest was left out.
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManualError {
    #[error("{0}")]
    Invalid(String),
    /// Another broadcast to everyone is still running.
    #[error("another notification to everyone is still being sent")]
    Busy,
    #[error("{0}")]
    Directory(String),
}

impl From<PortError> for ManualError {
    fn from(error: PortError) -> Self {
        ManualError::Directory(error.0)
    }
}

pub struct ManualPush {
    directory: Arc<dyn DeviceDirectory>,
    dispatcher: Dispatcher,
    /// One broadcast at a time.
    broadcast: Semaphore,
    patience: Duration,
}

fn validate(request: &ManualRequest) -> Result<(), ManualError> {
    let invalid = |message: &str| Err(ManualError::Invalid(message.to_owned()));
    let title = request.title.trim();
    let body = request.body.trim();
    if title.is_empty() || title.chars().count() > MAX_TITLE_CHARS {
        return invalid("The title must be 1 to 64 characters.");
    }
    if body.is_empty() || body.chars().count() > MAX_BODY_CHARS {
        return invalid("The body must be 1 to 240 characters.");
    }
    if title.chars().any(char::is_control) || body.chars().any(|c| c.is_control() && c != '\n') {
        return invalid("The text must not contain control characters.");
    }
    if let ManualTarget::User(id) = &request.target
        && (id.is_empty() || id.len() > 20 || !id.bytes().all(|b| b.is_ascii_digit()))
    {
        return invalid("The user id must be a number.");
    }
    Ok(())
}

impl ManualPush {
    pub fn new(directory: Arc<dyn DeviceDirectory>, dispatcher: Dispatcher) -> Self {
        Self {
            directory,
            dispatcher,
            broadcast: Semaphore::new(1),
            patience: QUEUE_PATIENCE,
        }
    }

    #[cfg(test)]
    fn with_patience(mut self, patience: Duration) -> Self {
        self.patience = patience;
        self
    }

    pub async fn send(&self, request: ManualRequest) -> Result<ManualReport, ManualError> {
        validate(&request)?;
        let push = OutboundPush::Alert(AlertContent {
            title: request.title.trim().to_owned(),
            body: request.body.trim().to_owned(),
            thread_id: "manual".to_owned(),
            data: vec![("type".to_owned(), "manual".to_owned())],
        });
        let actor: String = request
            .actor
            .iter()
            .flat_map(|a| a.chars())
            .take(MAX_ACTOR_CHARS)
            .collect();
        let report = match &request.target {
            ManualTarget::User(id) => {
                let devices = self.directory.user_devices(id).await?;
                let mut report = ManualReport {
                    devices: devices.len(),
                    ..ManualReport::default()
                };
                if !request.dry_run {
                    self.submit_all(devices, &push, &mut report).await;
                }
                report
            }
            ManualTarget::All => {
                let _permit = self.broadcast.try_acquire().map_err(|_| ManualError::Busy)?;
                self.broadcast_all(&push, request.dry_run).await?
            }
        };
        let (kind, user) = match &request.target {
            ManualTarget::All => ("all", ""),
            ManualTarget::User(id) => ("user", id.as_str()),
        };
        tracing::info!(
            target_kind = kind,
            target_user = user,
            %actor,
            title = %request.title.trim(),
            dry_run = request.dry_run,
            devices = report.devices,
            queued = report.queued,
            dropped = report.dropped,
            truncated = report.truncated,
            "manual push"
        );
        Ok(report)
    }

    async fn broadcast_all(&self, push: &OutboundPush, dry_run: bool) -> Result<ManualReport, ManualError> {
        let mut report = ManualReport::default();
        let mut after: Option<String> = None;
        loop {
            let page = self.directory.device_page(after.as_deref()).await?;
            let room = MAX_BROADCAST_DEVICES - report.devices;
            let mut devices = page.devices;
            if devices.len() > room {
                devices.truncate(room);
                report.truncated = true;
            }
            report.devices += devices.len();
            if !dry_run {
                self.submit_all(devices, push, &mut report).await;
            }
            if report.truncated {
                return Ok(report);
            }
            match page.next_after {
                None => return Ok(report),
                Some(next) if after.as_deref() == Some(next.as_str()) => {
                    return Err(ManualError::Directory(
                        "the api returned the same page twice".to_owned(),
                    ));
                }
                Some(next) => after = Some(next),
            }
        }
    }

    async fn submit_all(&self, devices: Vec<DeviceToken>, push: &OutboundPush, report: &mut ManualReport) {
        for device in devices {
            let provider = device.platform.label();
            match self
                .dispatcher
                .submit_waiting(device, push.clone(), self.patience)
                .await
            {
                Submitted::Queued => {
                    report.queued += 1;
                    metrics::counter!("notifier_pushes_total", "provider" => provider, "outcome" => "manual")
                        .increment(1);
                }
                Submitted::Dropped => report.dropped += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::dispatcher::DispatchConfig;
    use crate::application::ports::{DevicePage, Environment, Platform, PushSender, SendError, TokenRegistry};

    fn device(name: &str, platform: Platform) -> DeviceToken {
        DeviceToken {
            platform,
            token: name.into(),
            environment: Environment::Production,
            previews: true,
            only_mentions: false,
        }
    }

    /// Users in id order with their devices, served `page_size` users at a time.
    struct FakeDirectory {
        users: Vec<(String, Vec<DeviceToken>)>,
        page_size: usize,
    }

    #[async_trait]
    impl DeviceDirectory for FakeDirectory {
        async fn user_devices(&self, user_id: &str) -> Result<Vec<DeviceToken>, PortError> {
            Ok(self
                .users
                .iter()
                .find(|(id, _)| id == user_id)
                .map(|(_, devices)| devices.clone())
                .unwrap_or_default())
        }

        async fn device_page(&self, after: Option<&str>) -> Result<DevicePage, PortError> {
            let rest: Vec<_> = self
                .users
                .iter()
                .filter(|(id, _)| after.is_none_or(|a| id.as_str() > a))
                .take(self.page_size)
                .collect();
            let next_after = (rest.len() == self.page_size).then(|| rest.last().unwrap().0.clone());
            Ok(DevicePage {
                devices: rest.iter().flat_map(|(_, d)| d.clone()).collect(),
                next_after,
            })
        }
    }

    #[derive(Default)]
    struct FakeSender {
        sent: Mutex<Vec<(String, OutboundPush)>>,
    }

    #[async_trait]
    impl PushSender for FakeSender {
        async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError> {
            self.sent.lock().unwrap().push((token.token.clone(), push.clone()));
            Ok(())
        }
    }

    struct NoRegistry;

    #[async_trait]
    impl TokenRegistry for NoRegistry {
        async fn forget(&self, _token: &DeviceToken) -> Result<(), PortError> {
            Ok(())
        }
    }

    fn directory(users: &[(&str, &[&str])], page_size: usize) -> Arc<FakeDirectory> {
        Arc::new(FakeDirectory {
            users: users
                .iter()
                .map(|(id, tokens)| {
                    (
                        (*id).to_owned(),
                        tokens
                            .iter()
                            .enumerate()
                            .map(|(n, t)| device(t, if n % 2 == 0 { Platform::Apns } else { Platform::Fcm }))
                            .collect(),
                    )
                })
                .collect(),
            page_size,
        })
    }

    fn config(queue_capacity: usize, max_in_flight: usize) -> DispatchConfig {
        DispatchConfig {
            queue_capacity,
            max_in_flight,
            max_attempts: 1,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        }
    }

    fn service(dir: Arc<FakeDirectory>, queue: usize, in_flight: usize) -> (ManualPush, Arc<FakeSender>) {
        let sender = Arc::new(FakeSender::default());
        let dispatcher = Dispatcher::spawn(config(queue, in_flight), sender.clone(), Arc::new(NoRegistry));
        (ManualPush::new(dir, dispatcher), sender)
    }

    fn request(target: ManualTarget) -> ManualRequest {
        ManualRequest {
            target,
            title: "Maintenance".into(),
            body: "Voics is back at 22:00.".into(),
            dry_run: false,
            actor: Some("owner@example.com".into()),
        }
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn one_user_gets_the_notification_on_every_device() {
        let (manual, sender) = service(directory(&[("1", &["a", "b"]), ("2", &["c"])], 10), 100, 10);
        let report = manual.send(request(ManualTarget::User("1".into()))).await.unwrap();
        settle().await;
        assert_eq!((report.devices, report.queued, report.dropped), (2, 2, 0));
        let sent = sender.sent.lock().unwrap();
        let mut tokens: Vec<_> = sent.iter().map(|(t, _)| t.as_str()).collect();
        tokens.sort_unstable();
        assert_eq!(tokens, ["a", "b"]);
        let OutboundPush::Alert(alert) = &sent[0].1 else {
            panic!("an alert")
        };
        assert_eq!(
            (alert.title.as_str(), alert.body.as_str()),
            ("Maintenance", "Voics is back at 22:00.")
        );
        assert!(alert.data.contains(&("type".into(), "manual".into())));
    }

    #[tokio::test]
    async fn a_user_without_devices_is_a_report_of_zero_not_an_error() {
        let (manual, sender) = service(directory(&[("1", &["a"])], 10), 100, 10);
        let report = manual.send(request(ManualTarget::User("9".into()))).await.unwrap();
        settle().await;
        assert_eq!(report, ManualReport::default());
        assert!(sender.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn everyone_is_every_page_of_the_directory() {
        let users: Vec<(String, Vec<&str>)> = (1..=7).map(|n| (format!("{n:03}"), vec!["x", "y"])).collect();
        let refs: Vec<(&str, &[&str])> = users.iter().map(|(id, t)| (id.as_str(), t.as_slice())).collect();
        let (manual, sender) = service(directory(&refs, 3), 100, 10);
        let report = manual.send(request(ManualTarget::All)).await.unwrap();
        settle().await;
        assert_eq!(
            (report.devices, report.queued, report.dropped, report.truncated),
            (14, 14, 0, false)
        );
        assert_eq!(sender.sent.lock().unwrap().len(), 14);
    }

    #[tokio::test]
    async fn a_dry_run_counts_the_devices_and_sends_nothing() {
        let (manual, sender) = service(directory(&[("1", &["a", "b"]), ("2", &["c"])], 1), 100, 10);
        let mut dry = request(ManualTarget::All);
        dry.dry_run = true;
        let report = manual.send(dry).await.unwrap();
        settle().await;
        assert_eq!((report.devices, report.queued), (3, 0));
        assert!(sender.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_broadcast_waits_for_the_queue_instead_of_bypassing_it() {
        // Queue of 2 and one slot: five devices still all arrive because the broadcast is paced.
        let (manual, sender) = service(directory(&[("1", &["a", "b", "c"]), ("2", &["d", "e"])], 10), 2, 1);
        let report = manual.send(request(ManualTarget::All)).await.unwrap();
        settle().await;
        assert_eq!((report.queued, report.dropped), (5, 0));
        assert_eq!(sender.sent.lock().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn pushes_the_queue_never_has_room_for_are_dropped_and_counted() {
        struct Stuck;
        #[async_trait]
        impl PushSender for Stuck {
            async fn send(&self, _t: &DeviceToken, _p: &OutboundPush) -> Result<(), SendError> {
                std::future::pending().await
            }
        }
        let dispatcher = Dispatcher::spawn(config(1, 1), Arc::new(Stuck), Arc::new(NoRegistry));
        let manual = ManualPush::new(directory(&[("1", &["a", "b", "c", "d"])], 10), dispatcher)
            .with_patience(Duration::from_millis(20));
        let report = manual.send(request(ManualTarget::User("1".into()))).await.unwrap();
        assert_eq!(report.devices, 4);
        assert_eq!(report.queued + report.dropped, 4);
        assert!(report.dropped >= 1, "{report:?}");
    }

    #[tokio::test]
    async fn only_one_broadcast_runs_at_a_time() {
        let (manual, _) = service(directory(&[("1", &["a"])], 10), 100, 10);
        let _held = manual.broadcast.try_acquire().unwrap();
        assert_eq!(manual.send(request(ManualTarget::All)).await, Err(ManualError::Busy));
        // A single user is never blocked by a broadcast.
        assert!(manual.send(request(ManualTarget::User("1".into()))).await.is_ok());
    }

    #[tokio::test]
    async fn text_and_target_are_validated_before_anything_is_looked_up() {
        let (manual, sender) = service(directory(&[("1", &["a"])], 10), 100, 10);
        let bad = |edit: &dyn Fn(&mut ManualRequest)| {
            let mut r = request(ManualTarget::User("1".into()));
            edit(&mut r);
            r
        };
        let cases = [
            bad(&|r| r.title = "  ".into()),
            bad(&|r| r.title = "t".repeat(65)),
            bad(&|r| r.body = String::new()),
            bad(&|r| r.body = "b".repeat(241)),
            bad(&|r| r.title = "line\nbreak".into()),
            bad(&|r| r.target = ManualTarget::User("abc".into())),
            bad(&|r| r.target = ManualTarget::User(String::new())),
        ];
        for case in cases {
            assert!(
                matches!(manual.send(case.clone()).await, Err(ManualError::Invalid(_))),
                "{case:?}"
            );
        }
        let limits = bad(&|r| {
            r.title = "t".repeat(64);
            r.body = "b".repeat(240);
        });
        assert!(manual.send(limits).await.is_ok(), "exactly at the limits is fine");
        settle().await;
        assert_eq!(sender.sent.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_directory_failure_is_reported() {
        struct Down;
        #[async_trait]
        impl DeviceDirectory for Down {
            async fn user_devices(&self, _: &str) -> Result<Vec<DeviceToken>, PortError> {
                Err(PortError::new("api down"))
            }
            async fn device_page(&self, _: Option<&str>) -> Result<DevicePage, PortError> {
                Err(PortError::new("api down"))
            }
        }
        let dispatcher = Dispatcher::spawn(config(10, 1), Arc::new(FakeSender::default()), Arc::new(NoRegistry));
        let manual = ManualPush::new(Arc::new(Down), dispatcher);
        assert_eq!(
            manual.send(request(ManualTarget::All)).await,
            Err(ManualError::Directory("api down".into()))
        );
    }
}

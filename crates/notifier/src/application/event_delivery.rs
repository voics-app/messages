//! Durable, event-specific push delivery; it does not use the message push queue.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use super::ports::{DeviceToken, PortError, PushSender, SendError, TokenRegistry};
use crate::domain::{AlertContent, OutboundPush};

#[derive(Debug, Clone, Deserialize)]
pub struct EventDelivery {
    pub delivery_id: String,
    pub lease_id: String,
    pub guild_id: String,
    pub event_id: String,
    pub user_id: String,
    pub scheduled_start_time: String,
    pub revision: i64,
}

#[async_trait]
pub trait EventDeliveryDirectory: Send + Sync {
    async fn claim(&self) -> Result<Option<EventDelivery>, PortError>;
    async fn resolve(&self, delivery: &EventDelivery) -> Result<Option<DeviceToken>, PortError>;
    /// None acknowledges a terminal result; Some schedules a retry after that many seconds.
    async fn finish(&self, delivery: &EventDelivery, retry_after_secs: Option<u32>) -> Result<(), PortError>;
}

pub struct EventDeliveryWorker {
    directory: Arc<dyn EventDeliveryDirectory>,
    sender: Arc<dyn PushSender>,
    registry: Arc<dyn TokenRegistry>,
}

impl EventDeliveryWorker {
    pub fn new(
        directory: Arc<dyn EventDeliveryDirectory>,
        sender: Arc<dyn PushSender>,
        registry: Arc<dyn TokenRegistry>,
    ) -> Self {
        Self {
            directory,
            sender,
            registry,
        }
    }

    pub async fn run(self) {
        loop {
            match self.process_one().await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(Duration::from_secs(2)).await,
                Err(error) => {
                    tracing::warn!(%error, "event reminder delivery unavailable");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// One leased delivery. A crash or a failed acknowledgement leaves it reclaimable after lease expiry.
    pub async fn process_one(&self) -> Result<bool, PortError> {
        let Some(delivery) = self.directory.claim().await? else {
            return Ok(false);
        };
        let token = match self.directory.resolve(&delivery).await {
            Ok(Some(token)) => token,
            Ok(None) => {
                self.directory.finish(&delivery, None).await?;
                return Ok(true);
            }
            Err(error) => {
                self.directory.finish(&delivery, Some(10)).await?;
                return Err(error);
            }
        };
        let alert = AlertContent {
            title: "Event starting".into(),
            body: "An event you are interested in is starting.".into(),
            thread_id: delivery.delivery_id.clone(),
            data: vec![
                ("type".into(), "scheduled_event".into()),
                ("guild_id".into(), delivery.guild_id.clone()),
                ("event_id".into(), delivery.event_id.clone()),
                ("dedupe_key".into(), delivery.delivery_id.clone()),
            ],
        };
        let retry = match self.sender.send(&token, &OutboundPush::Alert(alert)).await {
            Ok(()) => None,
            Err(SendError::InvalidToken(_)) => match self.registry.forget(&token).await {
                Ok(()) => None,
                Err(_) => Some(10),
            },
            Err(SendError::Retry { after, .. }) => {
                Some(after.map_or(10, |delay| u32::try_from(delay.as_secs()).unwrap_or(60).clamp(1, 60)))
            }
            Err(SendError::Failed(_) | SendError::Unsupported(_)) => Some(10),
        };
        self.directory.finish(&delivery, retry).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{Environment, Platform};
    use std::sync::Mutex;

    struct Directory {
        claimed: Mutex<bool>,
        result: Mutex<Vec<Option<u32>>>,
        token: Option<DeviceToken>,
    }
    #[async_trait]
    impl EventDeliveryDirectory for Directory {
        async fn claim(&self) -> Result<Option<EventDelivery>, PortError> {
            let mut claimed = self.claimed.lock().unwrap();
            if *claimed {
                return Ok(None);
            }
            *claimed = true;
            Ok(Some(EventDelivery {
                delivery_id: "delivery".into(),
                lease_id: "lease".into(),
                guild_id: "1".into(),
                event_id: "2".into(),
                user_id: "3".into(),
                scheduled_start_time: "2026-12-01T12:00:00Z".into(),
                revision: 0,
            }))
        }
        async fn resolve(&self, _: &EventDelivery) -> Result<Option<DeviceToken>, PortError> {
            Ok(self.token.clone())
        }
        async fn finish(&self, _: &EventDelivery, retry: Option<u32>) -> Result<(), PortError> {
            self.result.lock().unwrap().push(retry);
            Ok(())
        }
    }
    struct Sender {
        fail: bool,
    }
    #[async_trait]
    impl PushSender for Sender {
        async fn send(&self, _: &DeviceToken, _: &OutboundPush) -> Result<(), SendError> {
            if self.fail {
                Err(SendError::Retry {
                    reason: "retry".into(),
                    after: Some(Duration::from_secs(7)),
                })
            } else {
                Ok(())
            }
        }
    }
    struct Registry;
    #[async_trait]
    impl TokenRegistry for Registry {
        async fn forget(&self, _: &DeviceToken) -> Result<(), PortError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn provider_retry_is_not_acknowledged_and_ineligible_delivery_is_suppressed() {
        let token = DeviceToken {
            platform: Platform::Fcm,
            token: "test".into(),
            environment: Environment::Production,
            previews: false,
            only_mentions: false,
        };
        let directory = Arc::new(Directory {
            claimed: Mutex::new(false),
            result: Mutex::new(Vec::new()),
            token: Some(token),
        });
        EventDeliveryWorker::new(directory.clone(), Arc::new(Sender { fail: true }), Arc::new(Registry))
            .process_one()
            .await
            .unwrap();
        assert_eq!(*directory.result.lock().unwrap(), vec![Some(7)]);
        let muted = Arc::new(Directory {
            claimed: Mutex::new(false),
            result: Mutex::new(Vec::new()),
            token: None,
        });
        EventDeliveryWorker::new(muted.clone(), Arc::new(Sender { fail: false }), Arc::new(Registry))
            .process_one()
            .await
            .unwrap();
        assert_eq!(*muted.result.lock().unwrap(), vec![None]);
    }
}

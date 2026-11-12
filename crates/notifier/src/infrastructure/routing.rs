//! Sends each token through the sender of its provider. A provider without credentials has no
//! sender: its tokens are skipped (counted `unsupported`) and the other provider keeps working.

use std::sync::Arc;

use async_trait::async_trait;

use crate::application::{DeviceToken, Platform, PushSender, SendError};
use crate::domain::OutboundPush;

pub struct RoutingSender {
    apns: Option<Arc<dyn PushSender>>,
    fcm: Option<Arc<dyn PushSender>>,
}

impl RoutingSender {
    pub fn new(apns: Option<Arc<dyn PushSender>>, fcm: Option<Arc<dyn PushSender>>) -> Self {
        Self { apns, fcm }
    }
}

#[async_trait]
impl PushSender for RoutingSender {
    async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError> {
        let sender = match token.platform {
            Platform::Apns => &self.apns,
            Platform::Fcm => &self.fcm,
        };
        match sender {
            Some(sender) => sender.send(token, push).await,
            None => Err(SendError::Unsupported(format!(
                "{} is not configured",
                token.platform.label()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::application::Environment;
    use crate::domain::AlertContent;

    struct Named(&'static str, Arc<Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl PushSender for Named {
        async fn send(&self, _token: &DeviceToken, _push: &OutboundPush) -> Result<(), SendError> {
            self.1.lock().unwrap().push(self.0);
            Ok(())
        }
    }

    fn token(platform: Platform) -> DeviceToken {
        DeviceToken {
            platform,
            token: "t".into(),
            environment: Environment::Production,
            previews: true,
            only_mentions: false,
        }
    }

    fn push() -> OutboundPush {
        OutboundPush::Alert(AlertContent {
            title: "t".into(),
            body: String::new(),
            thread_id: "1".into(),
            data: Vec::new(),
        })
    }

    #[tokio::test]
    async fn tokens_go_to_the_sender_of_their_provider() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let router = RoutingSender::new(
            Some(Arc::new(Named("apns", log.clone()))),
            Some(Arc::new(Named("fcm", log.clone()))),
        );
        router.send(&token(Platform::Fcm), &push()).await.unwrap();
        router.send(&token(Platform::Apns), &push()).await.unwrap();
        assert_eq!(*log.lock().unwrap(), vec!["fcm", "apns"]);
    }

    #[tokio::test]
    async fn a_provider_without_credentials_is_skipped_and_the_other_still_works() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let only_fcm = RoutingSender::new(None, Some(Arc::new(Named("fcm", log.clone()))));
        assert!(matches!(
            only_fcm.send(&token(Platform::Apns), &push()).await,
            Err(SendError::Unsupported(_))
        ));
        only_fcm.send(&token(Platform::Fcm), &push()).await.unwrap();
        let only_apns = RoutingSender::new(Some(Arc::new(Named("apns", log.clone()))), None);
        assert!(matches!(
            only_apns.send(&token(Platform::Fcm), &push()).await,
            Err(SendError::Unsupported(_))
        ));
        assert_eq!(*log.lock().unwrap(), vec!["fcm"]);
    }
}

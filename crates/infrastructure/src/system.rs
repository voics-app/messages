//! System clock and issuance from committed, bounded API reservations.
use crate::api::InternalApi;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{Clock, IdGenerator};
use discordy_messages_domain::{Snowflake, WorkerId};
use std::sync::Arc;
use std::time::Instant;
use voics_snowflake::{LeasedIds, RENEW_EVERY, ReservationRequest, Service};

pub struct SystemClock;
impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        voics_snowflake::unix_ms()
    }
}

pub struct SnowflakeIds {
    issuer: LeasedIds,
}
impl SnowflakeIds {
    pub async fn start(api: InternalApi, preferred: WorkerId) -> Result<Arc<Self>, PortError> {
        let requested_at = Instant::now();
        let reservation = api
            .reserve_ids(ReservationRequest::Acquire {
                service: Service::Messages,
                owner_id: uuid::Uuid::new_v4(),
                preferred_worker: preferred.value(),
            })
            .await?;
        let ids = Arc::new(Self {
            issuer: LeasedIds::new(reservation, requested_at).map_err(PortError::new)?,
        });
        let weak = Arc::downgrade(&ids);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RENEW_EVERY).await;
                let Some(ids) = weak.upgrade() else {
                    return;
                };
                let Ok(current) = ids.issuer.reservation() else {
                    return;
                };
                let requested_at = Instant::now();
                let result = api
                    .reserve_ids(ReservationRequest::Renew {
                        service: Service::Messages,
                        owner_id: current.owner_id,
                        worker_id: current.worker_id,
                    })
                    .await;
                match result {
                    Ok(next) => match ids.issuer.renew(next, requested_at) {
                        Ok(()) => {}
                        Err(voics_snowflake::IdError::Expired) => {
                            tracing::warn!("ID reservation expired; restart required");
                            return;
                        }
                        Err(_) => tracing::warn!("ID renewal rejected; committed bound unchanged"),
                    },
                    Err(_) => tracing::warn!("ID renewal unavailable; committed bound unchanged"),
                }
            }
        });
        tracing::info!(worker_id = reservation.worker_id, "reserved messages ID worker");
        Ok(ids)
    }
}
impl IdGenerator for SnowflakeIds {
    fn next_id(&self) -> Result<Snowflake, PortError> {
        self.issuer.next_id().map(Snowflake::new).map_err(PortError::new)
    }
    fn is_ready(&self) -> bool {
        self.issuer.is_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
    use std::sync::atomic::{AtomicU16, Ordering};

    #[tokio::test]
    async fn independent_replicas_reserve_different_workers_and_fail_closed_on_expiry() {
        let workers = Arc::new(AtomicU16::new(65));
        async fn acquire(
            State(workers): State<Arc<AtomicU16>>,
            headers: HeaderMap,
            Json(request): Json<ReservationRequest>,
        ) -> Json<voics_snowflake::Reservation> {
            assert_eq!(headers["x-internal-token"], "test-internal");
            let ReservationRequest::Acquire {
                service,
                owner_id,
                preferred_worker,
            } = request
            else {
                panic!("unexpected renewal");
            };
            assert_eq!(service, Service::Messages);
            assert_eq!(preferred_worker, 64);
            let now = voics_snowflake::unix_ms();
            Json(voics_snowflake::Reservation {
                worker_id: workers.fetch_add(1, Ordering::SeqCst),
                owner_id,
                start_ms: now,
                end_ms: now + 30_000,
                issued_at_ms: now,
            })
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/internal/v1/id-reservations", post(acquire))
                    .with_state(workers),
            )
            .await
            .unwrap()
        });
        let api = InternalApi::new(reqwest::Client::new(), &url, "test-internal".into());
        let first = SnowflakeIds::start(api.clone(), WorkerId::for_messages(64).unwrap())
            .await
            .unwrap();
        let second = SnowflakeIds::start(api, WorkerId::for_messages(64).unwrap())
            .await
            .unwrap();
        let now = voics_snowflake::unix_ms();
        let a = first.issuer.next_at(now, Instant::now()).unwrap();
        let b = second.issuer.next_at(now, Instant::now()).unwrap();
        assert_ne!(a, b);
        assert_eq!((a >> 12) & 1023, 65);
        assert_eq!((b >> 12) & 1023, 66);
        assert!(
            first
                .issuer
                .next_at(now, Instant::now() + std::time::Duration::from_secs(31))
                .is_err()
        );
        server.abort();
    }
}

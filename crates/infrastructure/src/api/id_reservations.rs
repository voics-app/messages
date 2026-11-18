use super::InternalApi;
use discordy_messages_application::PortError;
use voics_snowflake::{Reservation, ReservationRequest};

impl InternalApi {
    pub async fn reserve_ids(&self, request: ReservationRequest) -> Result<Reservation, PortError> {
        let response = self
            .post("/internal/v1/id-reservations")
            .json(&request)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| PortError::new("ID reservation API unavailable"))?;
        if !response.status().is_success() {
            return Err(PortError::new("ID reservation API refused the request"));
        }
        let reservation: Reservation = response
            .json()
            .await
            .map_err(|_| PortError::new("Invalid ID reservation response"))?;
        let (owner, worker) = match request {
            ReservationRequest::Acquire { owner_id, .. } => (owner_id, None),
            ReservationRequest::Renew {
                owner_id, worker_id, ..
            } => (owner_id, Some(worker_id)),
        };
        if reservation.owner_id != owner
            || !voics_snowflake::Service::Messages.allocatable(reservation.worker_id)
            || worker.is_some_and(|worker| worker != reservation.worker_id)
        {
            return Err(PortError::new("ID reservation identity mismatch"));
        }
        reservation.validate().map_err(PortError::new)?;
        Ok(reservation)
    }
}

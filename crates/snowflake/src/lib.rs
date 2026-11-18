//! Committed timestamp intervals, never reusable ownership leases alone.
//! Persistence and renewal belong to adapters; this crate enforces issuance bounds.
//! A fresh process must acquire with a fresh owner UUID before constructing one issuer.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const EPOCH_MS: u64 = 1_767_225_600_000;
/// Signed database bigint leaves 41 timestamp bits above worker+sequence.
pub const MAX_TIMESTAMP_MS: u64 = EPOCH_MS + ((1_u64 << 41) - 1);
pub const RESERVATION_MS: u64 = 30_000;
pub const RENEW_EVERY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Service {
    Api,
    Messages,
}

impl Service {
    pub fn contains(self, worker: u16) -> bool {
        match self {
            Self::Api => worker <= 63,
            Self::Messages => (64..=127).contains(&worker),
        }
    }

    /// Workers 0 and 64 belong permanently to legacy static-ID binaries.
    pub fn allocatable(self, worker: u16) -> bool {
        self.contains(worker) && worker != 0 && worker != 64
    }

    pub fn workers(self) -> std::ops::RangeInclusive<u16> {
        match self {
            Self::Api => 1..=63,
            Self::Messages => 65..=127,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReservationRequest {
    Acquire {
        service: Service,
        owner_id: Uuid,
        preferred_worker: u16,
    },
    Renew {
        service: Service,
        owner_id: Uuid,
        worker_id: u16,
    },
}

impl ReservationRequest {
    pub fn service(self) -> Service {
        match self {
            Self::Acquire { service, .. } | Self::Renew { service, .. } => service,
        }
    }
}

/// Returned only after the database transaction commits. End is exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub worker_id: u16,
    pub owner_id: Uuid,
    pub start_ms: u64,
    pub end_ms: u64,
    pub issued_at_ms: u64,
}

impl Reservation {
    pub fn validate(self) -> Result<(), IdError> {
        if self.owner_id.is_nil()
            || !(Service::Api.allocatable(self.worker_id)
                || Service::Messages.allocatable(self.worker_id))
            || self.start_ms < EPOCH_MS
            || self.start_ms > self.issued_at_ms
            || self.issued_at_ms >= self.end_ms
            || self.end_ms > MAX_TIMESTAMP_MS
            || self.end_ms - self.issued_at_ms > RESERVATION_MS
        {
            return Err(IdError::InvalidReservation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("invalid ID reservation")]
    InvalidReservation,
    #[error("ID reservation expired")]
    Expired,
    #[error("clock outside committed ID interval")]
    OutsideInterval,
    #[error("clock moved backwards")]
    ClockRollback,
    #[error("ID sequence exhausted for this millisecond")]
    SequenceExhausted,
    #[error("ID renewal changed owner or worker")]
    DifferentOwner,
    #[error("stale ID reservation renewal")]
    StaleRenewal,
    #[error("ID generator state unavailable")]
    StateUnavailable,
}

/// Pure state machine. Time and time since request-start are supplied by the caller.
pub struct Generator {
    reservation: Reservation,
    last_ms: Option<u64>,
    sequence: u16,
}

impl Generator {
    pub fn new(reservation: Reservation) -> Result<Self, IdError> {
        reservation.validate()?;
        Ok(Self {
            reservation,
            last_ms: None,
            sequence: 0,
        })
    }

    pub fn check(&self, now_ms: u64, elapsed: Duration) -> Result<(), IdError> {
        if elapsed >= Duration::from_millis(self.reservation.end_ms - self.reservation.issued_at_ms)
        {
            return Err(IdError::Expired);
        }
        if now_ms < self.reservation.start_ms || now_ms >= self.reservation.end_ms {
            return Err(IdError::OutsideInterval);
        }
        if self.last_ms.is_some_and(|last| now_ms < last) {
            return Err(IdError::ClockRollback);
        }
        Ok(())
    }

    pub fn next(&mut self, now_ms: u64, elapsed: Duration) -> Result<u64, IdError> {
        self.check(now_ms, elapsed)?;
        let sequence = if self.last_ms == Some(now_ms) {
            self.sequence
                .checked_add(1)
                .filter(|n| *n < 4096)
                .ok_or(IdError::SequenceExhausted)?
        } else {
            0
        };
        self.last_ms = Some(now_ms);
        self.sequence = sequence;
        Ok(((now_ms - EPOCH_MS) << 22)
            | (u64::from(self.reservation.worker_id) << 12)
            | u64::from(sequence))
    }

    pub fn renew(&mut self, next: Reservation) -> Result<(), IdError> {
        next.validate()?;
        let current = self.reservation;
        if next.owner_id != current.owner_id
            || next.worker_id != current.worker_id
            || next.start_ms != current.start_ms
        {
            return Err(IdError::DifferentOwner);
        }
        if next.end_ms < current.end_ms || next.issued_at_ms <= current.issued_at_ms {
            return Err(IdError::StaleRenewal);
        }
        self.reservation = next;
        Ok(())
    }
}

struct State {
    generator: Generator,
    requested_at: Instant,
}

/// Thread-safe runtime adapter shared by both services. The request-start anchor ensures
/// network delay and a suspended process consume, rather than extend, the usable interval.
pub struct LeasedIds {
    state: Mutex<State>,
}

impl LeasedIds {
    pub fn new(reservation: Reservation, requested_at: Instant) -> Result<Self, IdError> {
        Ok(Self {
            state: Mutex::new(State {
                generator: Generator::new(reservation)?,
                requested_at,
            }),
        })
    }

    pub fn next_id(&self) -> Result<u64, IdError> {
        self.next_at(unix_ms(), Instant::now())
    }

    pub fn next_at(&self, now_ms: u64, now: Instant) -> Result<u64, IdError> {
        let mut state = self.state.lock().map_err(|_| IdError::StateUnavailable)?;
        let elapsed = now
            .checked_duration_since(state.requested_at)
            .ok_or(IdError::StateUnavailable)?;
        state.generator.next(now_ms, elapsed)
    }

    pub fn is_ready(&self) -> bool {
        self.state.lock().ok().is_some_and(|state| {
            state
                .generator
                .check(unix_ms(), state.requested_at.elapsed())
                .is_ok()
        })
    }

    pub fn reservation(&self) -> Result<Reservation, IdError> {
        self.state
            .lock()
            .map(|state| state.generator.reservation)
            .map_err(|_| IdError::StateUnavailable)
    }

    pub fn renew(&self, reservation: Reservation, requested_at: Instant) -> Result<(), IdError> {
        let mut state = self.state.lock().map_err(|_| IdError::StateUnavailable)?;
        let current = state.generator.reservation;
        if state.requested_at.elapsed()
            >= Duration::from_millis(current.end_ms - current.issued_at_ms)
        {
            return Err(IdError::Expired);
        }
        // Never let a replayed response restart the expiry timer.
        if requested_at <= state.requested_at {
            return Err(IdError::StaleRenewal);
        }
        if requested_at.elapsed()
            >= Duration::from_millis(reservation.end_ms.saturating_sub(reservation.issued_at_ms))
        {
            return Err(IdError::Expired);
        }
        state.generator.renew(reservation)?;
        state.requested_at = requested_at;
        Ok(())
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: u64 = EPOCH_MS + 10_000;
    fn reservation(worker: u16, owner: u128, start: u64, end: u64) -> Reservation {
        Reservation {
            worker_id: worker,
            owner_id: Uuid::from_u128(owner),
            start_ms: start,
            end_ms: end,
            issued_at_ms: start,
        }
    }

    #[test]
    fn legacy_workers_and_invalid_intervals_are_never_usable() {
        for worker in [0, 64, 128] {
            assert!(Generator::new(reservation(worker, 1, NOW, NOW + 100)).is_err());
        }
        for (start, end) in [
            (EPOCH_MS - 1, NOW),
            (NOW, NOW),
            (NOW, NOW + RESERVATION_MS + 1),
            (MAX_TIMESTAMP_MS, MAX_TIMESTAMP_MS + 1),
        ] {
            assert!(Generator::new(reservation(1, 1, start, end)).is_err());
        }
    }

    #[test]
    fn boundaries_rollback_and_sequence_exhaustion_fail_closed() {
        let mut generator = Generator::new(reservation(65, 1, NOW, NOW + 100)).unwrap();
        assert_eq!(
            generator.next(NOW - 1, Duration::ZERO),
            Err(IdError::OutsideInterval)
        );
        let ids: std::collections::HashSet<_> = (0..4096)
            .map(|_| generator.next(NOW, Duration::ZERO).unwrap())
            .collect();
        assert_eq!(ids.len(), 4096);
        assert_eq!(
            generator.next(NOW, Duration::ZERO),
            Err(IdError::SequenceExhausted)
        );
        assert!(generator.next(NOW + 1, Duration::from_millis(1)).is_ok());
        assert_eq!(
            generator.next(NOW, Duration::from_millis(2)),
            Err(IdError::ClockRollback)
        );
        assert_eq!(
            generator.next(NOW + 100, Duration::from_millis(99)),
            Err(IdError::OutsideInterval)
        );
        assert_eq!(
            generator.next(NOW + 2, Duration::from_millis(100)),
            Err(IdError::Expired)
        );
    }

    #[test]
    fn reassigned_worker_and_resumed_old_process_have_disjoint_ids() {
        let mut old = Generator::new(reservation(1, 1, NOW, NOW + 100)).unwrap();
        let mut new = Generator::new(reservation(1, 2, NOW + 100, NOW + 200)).unwrap();
        let before = old.next(NOW + 99, Duration::from_millis(99)).unwrap();
        let after = new.next(NOW + 100, Duration::ZERO).unwrap();
        assert!(after > before);
        assert_eq!(
            old.next(NOW + 100, Duration::from_millis(100)),
            Err(IdError::Expired)
        );
        assert_eq!(
            old.next(NOW + 99, Duration::from_millis(100)),
            Err(IdError::Expired)
        );
        assert_eq!(old.renew(new.reservation), Err(IdError::DifferentOwner));
    }

    #[test]
    fn renewal_preserves_sequence_and_rejects_delayed_replays() {
        let start = Instant::now();
        let current = reservation(1, 1, NOW, NOW + 100);
        let ids = LeasedIds::new(current, start).unwrap();
        let before = ids.next_at(NOW + 1, start).unwrap();
        let next = Reservation {
            end_ms: NOW + 200,
            issued_at_ms: NOW + 50,
            ..current
        };
        ids.renew(next, start + Duration::from_millis(1)).unwrap();
        let after = ids
            .next_at(NOW + 1, start + Duration::from_millis(2))
            .unwrap();
        assert_eq!(after, before + 1);
        assert_eq!(ids.renew(current, start), Err(IdError::StaleRenewal));
        assert_eq!(
            ids.renew(next, start + Duration::from_millis(3)),
            Err(IdError::StaleRenewal),
            "a replayed payload cannot restart the deadline with a fresh request anchor"
        );
        assert_eq!(
            ids.next_at(NOW + 50, start + Duration::from_millis(151)),
            Err(IdError::Expired)
        );
    }

    #[test]
    fn delayed_initial_response_does_not_extend_the_interval() {
        let start = Instant::now();
        let ids = LeasedIds::new(reservation(65, 1, NOW, NOW + 100), start).unwrap();
        assert_eq!(
            ids.next_at(NOW + 1, start + Duration::from_millis(100)),
            Err(IdError::Expired)
        );
    }

    #[test]
    fn an_expired_runtime_cannot_be_revived_by_a_late_renewal() {
        let now = Instant::now();
        let current = reservation(65, 1, NOW, NOW + 100);
        let ids = LeasedIds::new(current, now - Duration::from_millis(101)).unwrap();
        let next = Reservation {
            end_ms: NOW + 200,
            issued_at_ms: NOW + 90,
            ..current
        };
        assert_eq!(ids.renew(next, now), Err(IdError::Expired));
        assert_eq!(ids.reservation().unwrap(), current);
    }
}

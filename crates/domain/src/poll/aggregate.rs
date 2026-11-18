use crate::DomainError;

use super::{PollDefinition, PollError};

pub const MAX_PARTICIPANTS: u32 = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    Open,
    Finalized,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PollVote {
    pub revision: i64,
    pub mask: u16,
}

impl PollVote {
    pub fn answers(&self) -> Vec<u8> {
        answers(self.mask)
    }
}

fn answers(mask: u16) -> Vec<u8> {
    (1..=10).filter(|answer| mask & (1 << (answer - 1)) != 0).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollAggregate {
    pub version: i64,
    pub counts: Vec<u32>,
    pub participants: u32,
    pub status: PollStatus,
    pub expires_at_ms: i64,
    pub allow_multiselect: bool,
}

/// Both rows are written together under the previous aggregate version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollTransition {
    pub aggregate: PollAggregate,
    pub voter: PollVote,
    pub changed: bool,
    previous_mask: u16,
}

impl PollTransition {
    pub fn added(&self) -> Vec<u8> {
        answers(self.voter.mask & !self.previous_mask)
    }

    pub fn removed(&self) -> Vec<u8> {
        answers(self.previous_mask & !self.voter.mask)
    }
}

impl PollAggregate {
    pub fn new(definition: &PollDefinition) -> Self {
        Self {
            version: 0,
            counts: vec![0; definition.answers.len()],
            participants: 0,
            status: PollStatus::Open,
            expires_at_ms: definition.expires_at_ms,
            allow_multiselect: definition.allow_multiselect,
        }
    }

    pub fn validate(&self) -> Result<(), PollError> {
        if self.version < 0
            || !(2..=10).contains(&self.counts.len())
            || self.participants > MAX_PARTICIPANTS
            || self.counts.iter().any(|count| *count > self.participants)
        {
            return Err(PollError::Corrupt);
        }
        Ok(())
    }

    fn ensure_open(&self, now_ms: i64) -> Result<(), PollError> {
        if self.status == PollStatus::Deleted {
            Err(PollError::Deleted)
        } else if self.status != PollStatus::Open || now_ms >= self.expires_at_ms {
            Err(PollError::Closed)
        } else {
            self.validate()
        }
    }

    fn selected_mask(&self, previous: u16, answer: u8, selected: bool) -> Result<u16, PollError> {
        if answer == 0 || usize::from(answer) > self.counts.len() {
            return Err(DomainError::invalid("answer_id", "unknown poll answer").into());
        }
        let bit = 1 << (answer - 1);
        Ok(match (selected, self.allow_multiselect) {
            (true, true) => previous | bit,
            (true, false) => bit,
            (false, _) => previous & !bit,
        })
    }

    pub fn vote(
        &self,
        stored: Option<&PollVote>,
        answer: u8,
        selected: bool,
        expected: i64,
        now_ms: i64,
    ) -> Result<PollTransition, PollError> {
        self.ensure_open(now_ms)?;
        let voter = stored.cloned().unwrap_or_default();
        if voter.revision < 0
            || voter.mask >> self.counts.len() != 0
            || (!self.allow_multiselect && voter.mask.count_ones() > 1)
        {
            return Err(PollError::Corrupt);
        }
        let mask = self.selected_mask(voter.mask, answer, selected)?;
        let mut next = PollTransition {
            aggregate: self.clone(),
            previous_mask: voter.mask,
            voter,
            changed: false,
        };
        if mask == next.voter.mask {
            return Ok(next);
        }
        if expected != next.voter.revision {
            return Err(PollError::Conflict);
        }
        if stored.is_none() {
            if self.participants == MAX_PARTICIPANTS {
                return Err(PollError::Capacity);
            }
            next.aggregate.participants += 1;
        }
        next.aggregate.version = self.version.checked_add(1).ok_or(PollError::Exhausted)?;
        next.voter.revision = next.voter.revision.checked_add(1).ok_or(PollError::Exhausted)?;
        next.voter.mask = mask;
        for id in next.removed() {
            let count = &mut next.aggregate.counts[usize::from(id - 1)];
            *count = count.checked_sub(1).ok_or(PollError::Corrupt)?;
        }
        for id in next.added() {
            next.aggregate.counts[usize::from(id - 1)] += 1;
        }
        next.aggregate.validate()?;
        next.changed = true;
        Ok(next)
    }

    /// The caller authorizes early closing; workers pass `early = false`.
    pub fn close(&self, now_ms: i64, early: bool) -> Result<Option<Self>, PollError> {
        if self.status == PollStatus::Deleted {
            return Err(PollError::Deleted);
        }
        if self.status == PollStatus::Finalized || (!early && now_ms < self.expires_at_ms) {
            return Ok(None);
        }
        self.validate()?;
        let mut closed = self.clone();
        closed.version = self.version.checked_add(1).ok_or(PollError::Exhausted)?;
        closed.status = PollStatus::Finalized;
        Ok(Some(closed))
    }

    pub fn delete(&self) -> Result<Option<Self>, PollError> {
        if self.status == PollStatus::Deleted {
            return Ok(None);
        }
        let mut deleted = self.clone();
        deleted.version = self.version.checked_add(1).ok_or(PollError::Exhausted)?;
        deleted.status = PollStatus::Deleted;
        deleted.counts.fill(0);
        deleted.participants = 0;
        Ok(Some(deleted))
    }
}

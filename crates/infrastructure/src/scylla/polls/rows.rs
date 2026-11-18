use discordy_messages_application::ports::{PollKey, PollSnapshotState, PollStoreResult};
use discordy_messages_domain::poll::{PollAggregate, PollError, PollStatus, PollVote};
use scylla::DeserializeRow;

#[derive(DeserializeRow)]
pub(super) struct StoredRow {
    pub row_id: i64,
    version: Option<i64>,
    expires_at: Option<i64>,
    allow_multiselect: Option<bool>,
    status: Option<i32>,
    counts: Option<Vec<i32>>,
    participants: Option<i32>,
    vote_version: Option<i64>,
    vote_mask: Option<i32>,
}

pub(super) const COLUMNS: &str =
    "row_id,version,expires_at,allow_multiselect,status,counts,participants,vote_version,vote_mask";

impl StoredRow {
    fn aggregate(self) -> Result<PollAggregate, PollError> {
        let state = PollAggregate {
            version: self.version.ok_or(PollError::Corrupt)?,
            expires_at_ms: self.expires_at.ok_or(PollError::Corrupt)?,
            allow_multiselect: self.allow_multiselect.ok_or(PollError::Corrupt)?,
            status: match self.status {
                Some(0) => PollStatus::Open,
                Some(1) => PollStatus::Finalized,
                Some(2) => PollStatus::Deleted,
                _ => return Err(PollError::Corrupt),
            },
            counts: self
                .counts
                .ok_or(PollError::Corrupt)?
                .into_iter()
                .map(|count| u32::try_from(count).map_err(|_| PollError::Corrupt))
                .collect::<Result<_, _>>()?,
            participants: u32::try_from(self.participants.ok_or(PollError::Corrupt)?)
                .map_err(|_| PollError::Corrupt)?,
        };
        state.validate()?;
        Ok(state)
    }

    fn voter(self) -> Result<PollVote, PollError> {
        let revision = self.vote_version.ok_or(PollError::Corrupt)?;
        let mask = self.vote_mask.ok_or(PollError::Corrupt)?;
        if revision < 1 || !(0..1024).contains(&mask) {
            return Err(PollError::Corrupt);
        }
        Ok(PollVote {
            revision,
            mask: mask as u16,
        })
    }
}

pub(super) fn snapshot(rows: Vec<StoredRow>) -> PollStoreResult<Option<PollSnapshotState>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut aggregate = None;
    let mut voter = None;
    for row in rows {
        if row.row_id == 0 {
            aggregate = Some(row.aggregate()?);
        } else {
            voter = Some(row.voter()?);
        }
    }
    let aggregate = aggregate.ok_or(PollError::Corrupt)?;
    if let Some(vote) = &voter
        && aggregate.status != PollStatus::Deleted
        && (vote.revision > aggregate.version
            || aggregate.participants == 0
            || vote.mask >= (1 << aggregate.counts.len())
            || (!aggregate.allow_multiselect && vote.mask.count_ones() > 1)
            || aggregate
                .counts
                .iter()
                .enumerate()
                .any(|(index, count)| vote.mask & (1 << index) != 0 && *count == 0))
    {
        return Err(PollError::Corrupt.into());
    }
    Ok(Some(PollSnapshotState { aggregate, voter }))
}

pub(super) type MetadataValues = (i64, i64, bool, i32, Vec<i32>, i32, i64, i64, i64);

pub(super) fn values(key: PollKey, state: &PollAggregate, previous: i64) -> MetadataValues {
    (
        state.version,
        state.expires_at_ms,
        state.allow_multiselect,
        match state.status {
            PollStatus::Open => 0,
            PollStatus::Finalized => 1,
            PollStatus::Deleted => 2,
        },
        state.counts.iter().map(|count| *count as i32).collect(),
        state.participants as i32,
        key.channel_id.value() as i64,
        key.message_id.value() as i64,
        previous,
    )
}

//! Poll shapes are pinned by generated contracts and canonical HTTP fixtures.
use contracts::poll::{MessagePollVoteAddEvent, MessagePollVoteRemoveEvent, PollCreate, PollSnapshot};
use discordy_messages_application::ports::{MessageEvent, MessageEventKind};
use discordy_messages_application::{PortError, polls::PollView};
use discordy_messages_domain::poll::{PollAggregate, PollDefinition, PollDraft, PollMedia, PollStatus, PollVote};
use serde_json::{Value, json};

use crate::error::ApiError;

pub(crate) fn draft(value: PollCreate) -> Result<PollDraft, ApiError> {
    Ok(PollDraft {
        question: value.question.text.to_string(),
        answers: value
            .answers
            .into_iter()
            .map(|answer| PollMedia {
                text: answer.poll_media.text.to_string(),
                emoji: answer.poll_media.emoji.map(|emoji| emoji.name.to_string()),
            })
            .collect(),
        duration_hours: u16::try_from(value.duration.get())
            .map_err(|_| ApiError::invalid("poll.duration", "must be 1 to 768 hours"))?,
        allow_multiselect: value.allow_multiselect,
    })
}

pub(crate) fn definition(poll: &PollDefinition) -> Value {
    json!({
        "question":{"text":poll.question},
        "answers":poll.answers.iter().enumerate().map(|(index,answer)|{
            let mut media=json!({"text":answer.text});
            if let Some(name)=&answer.emoji {media["emoji"]=json!({"name":name});}
            json!({"answer_id":index+1,"poll_media":media})
        }).collect::<Vec<_>>(),
        "expiry":crate::dto::rfc3339(poll.expires_at_ms as u64),
        "allow_multiselect":poll.allow_multiselect,"layout_type":1,
    })
}

#[derive(Clone, Copy)]
pub(crate) enum PollViewer<'a> {
    Unknown,
    Known(Option<&'a PollVote>),
}

pub(crate) fn results(state: &PollAggregate, viewer: PollViewer<'_>) -> Value {
    let mut result = json!({
        "version":state.version.to_string(),"is_finalized":state.status==PollStatus::Finalized,
        "answer_counts":state.counts.iter().enumerate().map(|(index,count)|{
            let mut entry=json!({"id":index+1,"count":count});
            if let PollViewer::Known(voter)=viewer { entry["me_voted"]=json!(voter.is_some_and(|vote|vote.mask & (1<<index)!=0)); }
            entry
        }).collect::<Vec<_>>(),
    });
    if let PollViewer::Known(voter) = viewer {
        result["vote_version"] = json!(voter.map_or(0, |vote| vote.revision).to_string());
    }
    result
}

pub(crate) fn snapshot(view: &PollView) -> Result<PollSnapshot, PortError> {
    let mut poll = definition(
        view.message
            .poll
            .as_ref()
            .ok_or_else(|| PortError::new("poll view lacks a definition"))?,
    );
    poll["results"] = results(&view.state.aggregate, PollViewer::Known(view.state.voter.as_ref()));
    serde_json::from_value(poll).map_err(PortError::new)
}

pub(crate) fn vote_event(event: &MessageEvent) -> Result<Value, PortError> {
    let MessageEventKind::PollVoteChanged {
        message_id,
        user_id,
        answer_id,
        selected,
        voter,
        results: state,
    } = &event.kind
    else {
        return Err(PortError::new("expected poll vote event"));
    };
    let mut value = json!({"channel_id":event.channel_id.to_string(),"message_id":message_id.to_string(),"user_id":user_id.to_string(),"answer_id":answer_id,"answer_ids":voter.answers(),"vote_version":voter.revision.to_string(),"results":results(state,PollViewer::Unknown)});
    if let Some(guild) = event.guild_id {
        value["guild_id"] = json!(guild.to_string());
    }
    if *selected {
        let validated: MessagePollVoteAddEvent = serde_json::from_value(value).map_err(PortError::new)?;
        serde_json::to_value(validated).map_err(PortError::new)
    } else {
        let validated: MessagePollVoteRemoveEvent = serde_json::from_value(value).map_err(PortError::new)?;
        serde_json::to_value(validated).map_err(PortError::new)
    }
}

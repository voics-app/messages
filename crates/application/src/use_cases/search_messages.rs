use std::sync::Arc;

use discordy_messages_domain::{
    ChannelId, DomainError, GuildId, HistoryCursor, Message, MessageId, PageLimit, SearchFilters, UserId,
};
use futures::stream::{self, StreamExt, TryStreamExt};

use super::{GetMessageHistory, MessagingPorts};
use crate::error::{AppError, AppResult};
use crate::ports::{
    AccessError, BackfillState, ChannelAccessProvider, IndexOp, MessageRepository, PinRepository, PinnedFilter,
    ReactionRepository, SearchIndex, SearchRequest,
};
use crate::rate_limit::RateLimiter;
use crate::views::MessageView;
use discordy_messages_domain::reaction::summarize;

/// Searches per user per window.
const RATE_LIMIT: u32 = 10;
const RATE_WINDOW_MS: u64 = 10_000;
/// Concurrent access checks and ScyllaDB reads per search.
const FAN_OUT: usize = 16;

/// Where to search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchScope {
    /// Every channel of the guild the caller can read.
    Guild(GuildId),
    /// One channel (a guild channel or a direct message).
    Channel(ChannelId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResultMessage {
    pub view: MessageView,
    /// True for the match, false for its context messages.
    pub hit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutcome {
    pub total_results: u64,
    /// One group per hit, oldest message first.
    pub groups: Vec<Vec<SearchResultMessage>>,
    /// The index is still being built from stored history.
    pub index_building: bool,
}

pub struct SearchMessages {
    access: Arc<dyn ChannelAccessProvider>,
    index: Arc<dyn SearchIndex>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    pins: Arc<dyn PinRepository>,
    history: GetMessageHistory,
    limiter: RateLimiter,
}

impl SearchMessages {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            index: ports.search.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            pins: ports.pins.clone(),
            history: GetMessageHistory::new(ports),
            limiter: RateLimiter::new(ports.clock.clone(), RATE_LIMIT, RATE_WINDOW_MS),
        }
    }

    pub async fn execute(
        &self,
        scope: SearchScope,
        caller: UserId,
        filters: SearchFilters,
    ) -> AppResult<SearchOutcome> {
        self.limiter
            .check(caller)
            .map_err(|retry_after_ms| AppError::RateLimited { retry_after_ms })?;
        let building = self.index.backfill_state().await? != BackfillState::Complete;
        let channels = self.readable_channels(scope, caller, &filters).await?;
        if channels.is_empty() {
            return Ok(SearchOutcome {
                total_results: 0,
                groups: Vec::new(),
                index_building: building,
            });
        }
        let pinned = self.pinned_filter(&channels, &filters).await?;
        if matches!(&pinned, Some(PinnedFilter::Only(ids)) if ids.is_empty()) {
            return Ok(SearchOutcome {
                total_results: 0,
                groups: Vec::new(),
                index_building: building,
            });
        }
        let context = filters.context;
        let page = self
            .index
            .search(&SearchRequest {
                channels,
                filters,
                pinned,
            })
            .await?;
        let found: Vec<Option<Message>> = stream::iter(page.hits.iter().copied())
            .map(|hit| async move { self.messages.find(hit.channel_id, hit.message_id).await })
            .buffered(FAN_OUT)
            .try_collect()
            .await?;
        let mut groups = Vec::with_capacity(found.len());
        let mut ghosts: Vec<(ChannelId, MessageId)> = Vec::new();
        for (hit, message) in page.hits.iter().zip(found) {
            match message {
                Some(message) => groups.push(self.group(message, caller, context).await?),
                None => ghosts.push((hit.channel_id, hit.message_id)),
            }
        }
        self.forget(&ghosts).await;
        Ok(SearchOutcome {
            total_results: page.total,
            groups,
            index_building: building,
        })
    }

    /// Channels of the scope the caller may search: VIEW_CHANNEL and
    /// READ_MESSAGE_HISTORY in a channel whose delivery mode is Standard.
    async fn readable_channels(
        &self,
        scope: SearchScope,
        caller: UserId,
        filters: &SearchFilters,
    ) -> AppResult<Vec<ChannelId>> {
        match scope {
            SearchScope::Channel(channel_id) => {
                let access = self.access.access(channel_id, caller).await?;
                access.ensure_can_read_history()?;
                access.ensure_standard_delivery()?;
                Ok(vec![channel_id])
            }
            SearchScope::Guild(guild_id) => {
                let candidates = self.index.guild_channels(guild_id, &filters.channel_ids).await?;
                let answers: Vec<(ChannelId, Result<bool, AccessError>)> = stream::iter(candidates.iter().copied())
                    .map(|channel_id| async move {
                        let allowed = self.access.access(channel_id, caller).await.map(|access| {
                            access.guild_id == Some(guild_id)
                                && access.ensure_can_read_history().is_ok()
                                && access.ensure_standard_delivery().is_ok()
                        });
                        (channel_id, allowed)
                    })
                    .buffered(FAN_OUT)
                    .collect()
                    .await;
                let mut allowed = Vec::new();
                let mut not_member = 0;
                for (channel_id, answer) in answers {
                    match answer {
                        Ok(true) => allowed.push(channel_id),
                        Ok(false) | Err(AccessError::UnknownChannel) => {}
                        Err(AccessError::NotMember) => not_member += 1,
                        Err(AccessError::Unavailable(error)) => return Err(AppError::Unavailable(error)),
                    }
                }
                if allowed.is_empty() && not_member > 0 && not_member == candidates.len() {
                    return Err(DomainError::MissingAccess.into());
                }
                Ok(allowed)
            }
        }
    }

    async fn pinned_filter(&self, channels: &[ChannelId], filters: &SearchFilters) -> AppResult<Option<PinnedFilter>> {
        let Some(pinned) = filters.pinned else {
            return Ok(None);
        };
        let lists: Vec<Vec<MessageId>> = stream::iter(channels.iter().copied())
            .map(|channel_id| async move {
                self.pins
                    .list(channel_id)
                    .await
                    .map(|pins| pins.into_iter().map(|pin| pin.message_id).collect::<Vec<_>>())
            })
            .buffered(FAN_OUT)
            .try_collect()
            .await?;
        let ids: Vec<MessageId> = lists.into_iter().flatten().collect();
        Ok(Some(if pinned {
            PinnedFilter::Only(ids)
        } else {
            PinnedFilter::Except(ids)
        }))
    }

    /// The hit with its reactions and up to `context` messages on each side.
    async fn group(&self, hit: Message, caller: UserId, context: u32) -> AppResult<Vec<SearchResultMessage>> {
        let channel_id = hit.channel_id;
        let hit_id = hit.id;
        let rows = self.reactions.for_message(channel_id, hit_id).await?;
        let mut group = Vec::new();
        if context > 0 {
            let limit = PageLimit::parse(Some(context))?;
            let mut before = self
                .history
                .page(channel_id, HistoryCursor::Before(hit_id), limit)
                .await?;
            before.reverse();
            group.extend(before.into_iter().map(context_message));
        }
        group.push(SearchResultMessage {
            view: MessageView {
                reactions: summarize(&rows, caller),
                message: hit,
            },
            hit: true,
        });
        if context > 0 {
            let limit = PageLimit::parse(Some(context))?;
            let mut after = self
                .history
                .page(channel_id, HistoryCursor::After(hit_id), limit)
                .await?;
            after.reverse();
            group.extend(after.into_iter().map(context_message));
        }
        Ok(group)
    }

    /// Drops index entries whose message no longer exists in ScyllaDB.
    async fn forget(&self, ghosts: &[(ChannelId, MessageId)]) {
        if ghosts.is_empty() {
            return;
        }
        let ops: Vec<IndexOp> = ghosts
            .iter()
            .map(|(channel_id, message_id)| IndexOp::Delete {
                channel_id: *channel_id,
                message_ids: vec![*message_id],
            })
            .collect();
        if let Err(error) = self.index.apply(&ops, None).await {
            tracing::warn!(%error, "failed to drop stale search entries");
        }
    }
}

fn context_message(message: Message) -> SearchResultMessage {
    SearchResultMessage {
        view: MessageView::without_reactions(message),
        hit: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use crate::ports::{Pin, PinRepository};
    use discordy_messages_domain::{DeliveryMode, Permissions, Snowflake};

    fn filters(content: &str) -> SearchFilters {
        SearchFilters {
            content: Some(content.to_owned()),
            ..SearchFilters::default()
        }
    }

    async fn setup() -> (Harness, SearchMessages) {
        let harness = Harness::new();
        let search = SearchMessages::new(&harness.ports);
        (harness, search)
    }

    async fn seeded(harness: &Harness, author: UserId, content: &str) -> Message {
        harness.clock.advance_ms(5);
        let message = harness.seed(author, content).await;
        harness.search.index_message(&message, Some(GUILD));
        message
    }

    fn contents(outcome: &SearchOutcome) -> Vec<Vec<(String, bool)>> {
        outcome
            .groups
            .iter()
            .map(|g| g.iter().map(|m| (m.view.message.content.clone(), m.hit)).collect())
            .collect()
    }

    #[tokio::test]
    async fn finds_matches_with_context_in_a_channel() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "morning everyone").await;
        seeded(&harness, BOB, "the deploy is done").await;
        seeded(&harness, ALICE, "great").await;
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), BOB, filters("deploy"))
            .await
            .unwrap();
        assert_eq!(outcome.total_results, 1);
        assert_eq!(
            contents(&outcome),
            vec![vec![
                ("morning everyone".to_owned(), false),
                ("the deploy is done".to_owned(), true),
                ("great".to_owned(), false),
            ]]
        );
    }

    #[tokio::test]
    async fn context_can_be_disabled() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "one").await;
        seeded(&harness, ALICE, "two target").await;
        let mut wanted = filters("target");
        wanted.context = 0;
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, wanted)
            .await
            .unwrap();
        assert_eq!(contents(&outcome), vec![vec![("two target".to_owned(), true)]]);
    }

    #[tokio::test]
    async fn a_channel_search_needs_read_access() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "secret plans").await;
        harness
            .access
            .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES);
        let error = search
            .execute(SearchScope::Channel(CHANNEL), BOB, filters("secret"))
            .await
            .unwrap_err();
        assert_eq!(error, AppError::Domain(DomainError::MissingAccess));
    }

    #[tokio::test]
    async fn ephemeral_and_p2p_channels_are_not_searchable_on_the_server() {
        let (harness, search) = setup().await;
        for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
            harness.access.set(Ok(dm_access(mode)));
            let error = search
                .execute(SearchScope::Channel(CHANNEL), ALICE, filters("hello"))
                .await
                .unwrap_err();
            assert_eq!(error, AppError::Domain(DomainError::DeliveryModeConflict(mode)));
        }
    }

    #[tokio::test]
    async fn guild_search_skips_channels_the_caller_cannot_read() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "visible hello").await;
        // Same keyword in a second channel the caller cannot see.
        let other = Snowflake::new(2 << 22);
        harness.access.set_permissions(MEMBER);
        let hidden = Message::compose(
            harness.ports.ids.next_id().unwrap(),
            other,
            ALICE,
            discordy_messages_domain::MessageDraft {
                content: "hidden hello".into(),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        harness.messages.insert(&hidden).await.unwrap();
        harness.search.index_message(&hidden, Some(GUILD));
        harness.access.deny_channel(other);
        let outcome = search
            .execute(SearchScope::Guild(GUILD), BOB, filters("hello"))
            .await
            .unwrap();
        assert_eq!(outcome.total_results, 1);
        assert_eq!(
            outcome.groups[0]
                .iter()
                .find(|m| m.hit)
                .unwrap()
                .view
                .message
                .channel_id,
            CHANNEL
        );
    }

    #[tokio::test]
    async fn a_non_member_of_every_channel_gets_missing_access() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "hello there").await;
        harness.access.set(Err(AccessError::NotMember));
        let error = search
            .execute(SearchScope::Guild(GUILD), BOB, filters("hello"))
            .await
            .unwrap_err();
        assert_eq!(error, AppError::Domain(DomainError::MissingAccess));
    }

    #[tokio::test]
    async fn deleted_messages_are_dropped_and_forgotten() {
        let (harness, search) = setup().await;
        let message = seeded(&harness, ALICE, "ghost story").await;
        harness.messages.delete(CHANNEL, &[message.id]).await.unwrap();
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, filters("ghost"))
            .await
            .unwrap();
        assert!(outcome.groups.is_empty());
        assert_eq!(harness.search.len(), 0, "the stale entry is removed");
    }

    #[tokio::test]
    async fn pinned_filters_use_the_channel_pins() {
        let (harness, search) = setup().await;
        let pinned = seeded(&harness, ALICE, "pin me hello").await;
        seeded(&harness, ALICE, "other hello").await;
        harness
            .pins
            .pin(
                CHANNEL,
                Pin {
                    message_id: pinned.id,
                    pinned_at_ms: 1,
                },
            )
            .await
            .unwrap();
        let mut only = filters("hello");
        only.pinned = Some(true);
        only.context = 0;
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, only)
            .await
            .unwrap();
        assert_eq!(contents(&outcome), vec![vec![("pin me hello".to_owned(), true)]]);
        let mut except = filters("hello");
        except.pinned = Some(false);
        except.context = 0;
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, except)
            .await
            .unwrap();
        assert_eq!(contents(&outcome), vec![vec![("other hello".to_owned(), true)]]);
    }

    #[tokio::test]
    async fn searches_are_rate_limited_per_user() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "hello").await;
        for _ in 0..RATE_LIMIT {
            search
                .execute(SearchScope::Channel(CHANNEL), ALICE, filters("hello"))
                .await
                .unwrap();
        }
        let error = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, filters("hello"))
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::RateLimited { .. }));
        search
            .execute(SearchScope::Channel(CHANNEL), BOB, filters("hello"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reports_while_the_index_is_building() {
        let (harness, search) = setup().await;
        seeded(&harness, ALICE, "hello").await;
        harness.search.set_state(BackfillState::Running);
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, filters("hello"))
            .await
            .unwrap();
        assert!(outcome.index_building);
        harness.search.set_state(BackfillState::Complete);
        let outcome = search
            .execute(SearchScope::Channel(CHANNEL), ALICE, filters("hello"))
            .await
            .unwrap();
        assert!(!outcome.index_building);
    }
}
